#!/usr/bin/env python3
"""
Pipeline único: categoria + produto -> Shopee -> 10 mais vendidos -> cards (PNG).

Projeto paralelo e autossuficiente: tudo mora neste arquivo (vocabulário dos
nichos, cliente da API, template do card e fotografia). A única dependência
externa de configuração é o `.env`.

`.env` (na pasta deste arquivo ou na pasta de onde o script é executado):
    SHOPEE_APP_ID=...
    SHOPEE_APP_SECRET=...
    SHOPEE_API_URL=https://open-api.affiliate.shopee.com.br/graphql   # opcional

Instalação (uma vez só):
    pip install httpx playwright
    playwright install chromium

Uso:
    python pipeline_cards.py audio "fone bluetooth"
    python pipeline_cards.py jogos "controle xbox"
    python pipeline_cards.py audio                    # sem produto: 1ª busca da categoria
    python pipeline_cards.py --categorias             # lista os slugs

Saída: saida/<categoria>_<produto>/{produtos.json, png/}
(o HTML de cada card é só intermediário: vive numa pasta temporária e é apagado)
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import os
import random
import re
import sys
import tempfile
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

LIMITE = 20
PAGE_SIZE = 50  # teto da API
MAX_PAGES = 2  # os 100 mais vendidos
ENDPOINT_PADRAO = "https://open-api.affiliate.shopee.com.br/graphql"
SORT_BY_SALES = 2  # verificado contra a API real; não existe ordenação por desconto


# ==========================================================================
# Vocabulário compartilhado (casamento de termos no título)
# ==========================================================================

# O que não é produto físico com desconto em nicho nenhum. Curta de propósito:
# num canal multi-nicho, toda palavra a mais aqui reprova a mercadoria de
# alguma categoria, e o gargalo continua sendo escassez de oferta boa.
GLOBAL_BLOCKED_TERMS: tuple[str, ...] = (
    # Não é mercadoria: o canal anuncia preço "de/por" de coisa que chega pelo
    # correio, e nada disso chega.
    "arquivo digital",
    "ebook",
    "e book",
    "curso online",
    "aula online",
    "planilha editavel",
    "arte digital",
    "servico de",
    "conserto",
    "assistencia tecnica",
    # Artesanato e festa: ticket de centavos e desconto inflado, em qualquer
    # busca. Entram por "kit", "molde" e "personalizado", nunca pelo produto.
    "topper",
    "lembrancinha",
    "painel de festa",
    "papel de arroz",
    "molde para",
    # Falsificação declarada no próprio anúncio.
    "replica",
    "primeira linha aaa",
    # O canal é aberto e sem aviso de idade.
    "sex shop",
    "sexshop",
    "erotico",
    "afrodisiaco",
    "plug anal",
)

# Etiqueta que o vendedor cola no começo do próprio anúncio: "[Lswbd]",
# "[Entrega Rápida BRY8]", "[2pcs-nicethings1]". Mora aqui, e não em
# `similarity.py`, porque é vocabulário de vitrine como o resto do módulo — e
# `similarity` já importa daqui, então o caminho contrário seria import circular.
SELLER_TAG = re.compile(r"^\s*(?:\[[^\]]{1,20}\]\s*)+")


def normalize(text: str) -> str:
    """Minúsculas, sem acento, sem pontuação, espaços colapsados.

    A pontuação vira espaço para que "Air-Fryer" e "Notebook/Tablet" casem igual
    às versões escritas com espaço.
    """
    decomposto = unicodedata.normalize("NFKD", text)
    sem_acento = "".join(c for c in decomposto if not unicodedata.combining(c))
    return re.sub(r"[^0-9a-z]+", " ", sem_acento.casefold()).strip()


def compile_term(term: str) -> re.Pattern[str]:
    """Casa o termo no início de uma palavra.

    `\\b` só no começo (e não no fim) para que "camiseta" também case
    "camisetas": é como se pega o plural sem manter uma lista de flexões. O
    preço é casar prefixo de palavra maior ("tenis" dentro de "tenista"), o que
    na prática não aparece em título de produto. Sem `\\b` no começo, porém,
    "meia" casaria dentro de "colmeia" — e isso aparece.
    """
    return re.compile(rf"\b{re.escape(term)}")


def compile_terms(terms: tuple[str, ...]) -> tuple[tuple[str, re.Pattern[str]], ...]:
    """Pré-compila uma lista de termos, preservando o texto de cada um.

    O termo volta junto do padrão porque quem chama quer saber **qual** casou:
    é isso que vira o motivo no log de reprovação.
    """
    return tuple((term, compile_term(term)) for term in terms)


def first_match(
    normalized_title: str, patterns: tuple[tuple[str, re.Pattern[str]], ...]
) -> str | None:
    """Primeiro termo da lista encontrado no título **já normalizado**."""
    for term, pattern in patterns:
        if pattern.search(normalized_title):
            return term
    return None


def has_term(title: str, term: str) -> bool:
    """O título contém o termo obrigatório da busca curada?

    `re.compile` mantém cache interno das últimas centenas de padrões, então
    compilar a cada chamada não custa nada aqui.
    """
    return compile_term(normalize(term)).search(normalize(title)) is not None


_GLOBAL_BLOCKED = compile_terms(GLOBAL_BLOCKED_TERMS)


def globally_blocked_term(title: str) -> str | None:
    """Primeiro termo proibido no canal inteiro, ou None.

    A proibição por categoria é aplicada em `categories.blocked_term`, que
    chama esta função primeiro.
    """
    return first_match(normalize(title), _GLOBAL_BLOCKED)


# ==========================================================================
# Os nichos: um `Category` por nicho
# ==========================================================================

@dataclass(frozen=True)
class SearchQuery:
    """Uma busca enviada à Shopee, com termo obrigatório opcional.

    `required_term` troca o filtro de nicho por um **termo obrigatório** no
    título. Existe para a busca que pede um produto pela marca: a busca da
    Shopee é difusa e devolve a concorrência junto, então exigir a marca no
    título é o que mantém a busca curada sendo curada.
    """

    keyword: str
    required_term: str | None = None


@dataclass(frozen=True)
class Category:
    """Um nicho do canal, com tudo que o define."""

    slug: str
    label: str
    emoji: str
    niche_terms: tuple[str, ...]
    searches: tuple[SearchQuery, ...]
    ambiguous_terms: tuple[tuple[str, tuple[str, ...]], ...] = ()
    blocked_terms: tuple[str, ...] = ()

    @property
    def tag(self) -> str:
        return f"{self.emoji} #{self.slug}"


AUDIO = Category(
    slug="audio",
    label="Áudio",
    emoji="🎧",
    niche_terms=(
        "fone",
        "headset",
        "headphone",
        "earbud",
        "caixa de som",
        "soundbar",
        "amplificador",
        "microfone",
        "alto falante",
        "subwoofer",
        "monitor de audio",
        "interface de audio",
        "caneta limpadora",
    ),
    searches=(
        SearchQuery("fone de ouvido bluetooth"),
        SearchQuery("caixa de som bluetooth"),
        SearchQuery("soundbar"),
        SearchQuery("microfone condensador"),
        SearchQuery("amplificador bluetooth"),
        SearchQuery("fone de ouvido com fio"),
        SearchQuery("interface de áudio"),
        SearchQuery("caixa de som amplificada"),
        SearchQuery("caneta limpadora fone de ouvido"),
    ),
)

SAPATOS = Category(
    slug="sapatos",
    label="Sapatos Masculinos",
    emoji="👟",
    niche_terms=(
        "sapato",
        "bota",
        "coturno",
        "mocassim",
        "loafer",
        "sapatenis",
    ),
    ambiguous_terms=(
        # "tenis" -> tênis de mesa, tênis de campo. Exige acompanhante esportivo
        # ou indicador de gênero para contar como calçado.
        ("tenis", ("masculino", "esportivo", "corrida", "casual", "couro", "running")),
        # "sandalia" -> sandália feminina é comum; exige indicador neutro/masc.
        ("sandalia", ("masculino", "slide", "havaianas")),
        # "chinelo" -> chinelo de banho, infantil. Exige contexto masculino.
        ("chinelo", ("masculino", "slide", "havaianas", "adulto")),
    ),
    blocked_terms=("infantil", "bebe"),
    searches=(
        SearchQuery("tênis masculino"),
        SearchQuery("sapato social masculino"),
        SearchQuery("bota masculina"),
        SearchQuery("sapatênis masculino"),
        SearchQuery("chinelo masculino slide"),
    ),
)

CASA = Category(
    slug="casa",
    label="Casa e Construção",
    emoji="🏠",
    niche_terms=(
        # Cama, mesa e banho
        "jogo de cama",
        "lencol",
        "fronha",
        "edredom",
        "cobertor",
        "manta",
        "colcha",
        "toalha de banho",
        "toalha de rosto",
        "travesseiro",
        "protetor de colchao",
        # Decoração e conforto
        "cortina",
        "tapete",
        "capacho",
        "almofada",
        "puff",
        "luminaria",
        "abajur",
        "fita led",
        "espelho decorativo",
        "quadro decorativo",
        "vaso decorativo",
        # Organização
        "organizador",
        "caixa organizadora",
        "cesto",
        "prateleira",
        "nicho",
        "cabideiro",
        "cabide",
        "porta temperos",
        "dispenser",
        # Cozinha não elétrica
        "panela",
        "frigideira",
        "cacarola",
        "assadeira",
        "talheres",
        "tabua de corte",
        "pote hermetico",
        "garrafa termica",
        "copo termico",
        "caneca",
        "taca",
        "escorredor",
        "ralador",
        "descascador",
        # Limpeza manual
        "lixeira",
        "rodo",
        "vassoura",
        "esfregao",
        "balde",
        "varal",
        # Ferramentas e construção
        "furadeira",
        "parafusadeira",
        "serra",
        "alicate",
        "martelo",
        "nivel",
        "lixa",
        "extensao eletrica",
        "kit ferramentas",
    ),
    ambiguous_terms=(
        # "jogo" -> videogame, jogo de tabuleiro.
        ("jogo", ("cama", "toalha", "banho", "lencol", "panelas", "talheres", "jantar", "copos")),
        # "forma" -> "de forma prática", "forma física".
        ("forma", ("bolo", "gelo", "silicone", "pudim", "pizza", "cupcake")),
        # "faca" -> o verbo "faça", indistinguível após `normalize`.
        ("faca", ("cozinha", "chef", "inox", "churrasco", "ceramica", "pao", "cutelo")),
    ),
    blocked_terms=("adesivo de parede", "papel de parede", "papel contact"),
    searches=(
        SearchQuery("jogo de cama casal"),
        SearchQuery("jogo de panelas antiaderente"),
        SearchQuery("organizador de armário"),
        SearchQuery("tapete para sala"),
        SearchQuery("toalha de banho gigante"),
        SearchQuery("cortina blackout"),
        SearchQuery("garrafa térmica"),
        SearchQuery("furadeira parafusadeira"),
    ),
)

CELULARES = Category(
    slug="celulares",
    label="Celulares e Dispositivos",
    emoji="📱",
    niche_terms=(
        "smartphone",
        "celular",
        "tablet",
        "smartwatch",
        "smartband",
        "relogio inteligente",
        "iphone",
        "apple watch",
        "airpods",
        "power bank",
        "carregador portatil",
        "carregador por inducao",
        "carregador sem fio",
        "cabo usb",
        "suporte veicular",
    ),
    # Acessórios de celular têm ticket baixo e desconto inflado. A categoria
    # anuncia o **dispositivo**, não a capinha nem a película. Carregador, cabo
    # e suporte ficam de fora dessa restrição: são acessórios de alto giro que
    # o canal decidiu publicar mesmo assim.
    blocked_terms=(
        "capinha",
        "capa de celular",
        "capa para celular",
        "pelicula",
        "popsocket",
        "pop socket",
    ),
    searches=(
        SearchQuery("smartphone samsung galaxy"),
        SearchQuery("celular motorola moto g"),
        SearchQuery("tablet android"),
        SearchQuery("smartwatch"),
        SearchQuery("xiaomi redmi"),
        SearchQuery("iphone"),
        SearchQuery("apple watch"),
        SearchQuery("airpods"),
        SearchQuery("carregador portátil power bank"),
        SearchQuery("carregador por indução"),
        SearchQuery("cabo usb trançado reforçado"),
        SearchQuery("suporte veicular para smartphone"),
    ),
)

ELETRODOMESTICOS = Category(
    slug="eletrodomesticos",
    label="Eletrodomésticos",
    emoji="🔌",
    niche_terms=(
        # Cozinha
        "air fryer",
        "airfryer",
        "fritadeira",
        "liquidificador",
        "batedeira",
        "mixer",
        "processador de alimentos",
        "multiprocessador",
        "cafeteira",
        "panela eletrica",
        "panela de pressao eletrica",
        "micro ondas",
        "microondas",
        "forno eletrico",
        "sanduicheira",
        "torradeira",
        "grill eletrico",
        "espremedor",
        "extrator de suco",
        "chaleira eletrica",
        "cooktop",
        "fogao",
        "coifa",
        "depurador",
        # Limpeza
        "aspirador de po",
        "robo aspirador",
        "vassoura eletrica",
        "lavadora de alta pressao",
        "maquina de lavar",
        "lava e seca",
        "lava loucas",
        "tanquinho",
        "secadora",
        # Frio e clima
        "geladeira",
        "refrigerador",
        "freezer",
        "frigobar",
        "adega climatizada",
        "ventilador",
        "circulador de ar",
        "ar condicionado",
        "climatizador",
        "umidificador",
        "desumidificador",
        "aquecedor",
        # Água e roupa
        "purificador de agua",
        "bebedouro",
        "filtro de agua",
        "ferro de passar",
        "vaporizador de roupas",
        "maquina de costura",
    ),
    blocked_terms=(
        "resistencia para",
        "peca de reposicao",
        "capa de protecao",
        "capa para air fryer",
        "reparo",
    ),
    searches=(
        SearchQuery("air fryer"),
        SearchQuery("liquidificador"),
        SearchQuery("cafeteira expresso"),
        SearchQuery("aspirador de pó"),
        SearchQuery("robô aspirador"),
        SearchQuery("panela de pressão elétrica"),
        SearchQuery("sanduicheira grill"),
        SearchQuery("ferro de passar a vapor"),
        SearchQuery("purificador de água"),
        SearchQuery("batedeira planetária"),
        SearchQuery("ventilador de mesa"),
        SearchQuery("micro-ondas"),
        SearchQuery("air fryer mondial", "mondial"),
        SearchQuery("umidificador de ar"),
        SearchQuery("lava e seca"),
    ),
)

COMPUTADORES = Category(
    slug="computadores",
    label="Computadores e Acessórios",
    emoji="💻",
    niche_terms=(
        "notebook",
        "computador",
        "ultrabook",
        "macbook",
        "ipad",
        "processador",
        "ssd",
        "hd externo",
        "pen drive",
        "pendrive",
        "cartao de memoria",
        "memoria ram",
        "gabinete",
        "water cooler",
        "mouse",
        "mousepad",
        "teclado",
        "monitor",
        "webcam",
        "impressora",
        "scanner",
        "cartucho",
        "toner",
        "roteador",
        "repetidor de sinal",
        "modem",
        "hub usb",
        "nobreak",
        "estabilizador",
        "suporte para notebook",
        "suporte notebook",
    ),
    ambiguous_terms=(
        # "placa" -> placa de carro, placa de sinalização.
        ("placa", ("video", "mae", "som", "rede", "captura", "wifi")),
        # "fonte" -> fonte tipográfica, fonte de água.
        ("fonte", ("atx", "notebook", "alimentacao", "chaveada", "modular", "watts")),
        # "memoria" -> memória afetiva, jogo da memória.
        ("memoria", ("ram", "ddr", "cartao", "sd", "flash", "interna")),
        # "bateria" -> instrumento, escola de samba.
        ("bateria", ("notebook", "celular", "drone", "recarregavel", "litio", "lithium")),
    ),
    # Acessório de celular é ruído nesta categoria.
    blocked_terms=(
        "capinha",
        "capa de celular",
        "pelicula",
        "popsocket",
    ),
    searches=(
        SearchQuery("notebook gamer"),
        SearchQuery("ssd 1tb"),
        SearchQuery("mouse sem fio"),
        SearchQuery("teclado mecânico"),
        SearchQuery("monitor full hd"),
        SearchQuery("placa de vídeo rtx"),
        SearchQuery("pen drive 128gb"),
        SearchQuery("pen drive sandisk ultra", "sandisk"),
        SearchQuery("macbook air"),
        SearchQuery("ipad"),
        SearchQuery("suporte para notebook"),
        SearchQuery("memória ram ddr4 16gb"),
    ),
)

JOGOS = Category(
    slug="jogos",
    label="Jogos e Consoles",
    emoji="🎮",
    niche_terms=(
        "playstation",
        "xbox",
        "nintendo",
        "ps4",
        "ps5",
        "joystick",
        "gamepad",
        "headset gamer",
        "cadeira gamer",
        "controle de videogame",
        "controle sem fio",
        "video game",
        "videogame",
        "dualsense",
        "volante",
    ),
    searches=(
        SearchQuery("playstation 5"),
        SearchQuery("xbox series x"),
        SearchQuery("nintendo switch"),
        SearchQuery("controle sem fio xbox"),
        SearchQuery("headset gamer"),
        SearchQuery("cadeira gamer"),
        SearchQuery("volante para jogos pc console"),
    ),
)

CAMERAS = Category(
    slug="cameras",
    label="Câmeras e Drones",
    emoji="📸",
    niche_terms=(
        "camera mirrorless",
        "camera digital",
        "camera dslr",
        "camera de seguranca",
        "camera ip",
        "gopro",
        "drone",
        "lente para camera",
        "tripe fotografico",
        "flash fotografico",
        "gimbal",
        "ring light",
        "softbox",
    ),
    searches=(
        SearchQuery("câmera mirrorless"),
        SearchQuery("drone fpv"),
        SearchQuery("câmera de segurança wifi"),
        SearchQuery("gopro hero"),
        SearchQuery("ring light profissional"),
        SearchQuery("tripé para câmera"),
        SearchQuery("câmera digital"),
    ),
)

SUPLEMENTOS = Category(
    slug="suplementos",
    label="Suplementos",
    emoji="💪",
    niche_terms=(
        "suplemento",
        "whey protein",
        "creatina",
        "bcaa",
        "glutamina",
        "pre treino",
        "albumina",
        "caseina",
        "maltodextrina",
        "hipercalorico",
        "termogenico",
        "multivitaminico",
        "coqueteleira",
    ),
    searches=(
        SearchQuery("suplementos"),
        SearchQuery("suplementos academia"),
        SearchQuery("whey protein"),
        SearchQuery("creatina"),
        SearchQuery("pré treino"),
        SearchQuery("bcaa"),
    ),
)

# A ordem importa: é ela que o despachante percorre no rodízio, então é a ordem
# em que as categorias se alternam no canal.
CATEGORIES: tuple[Category, ...] = (
    AUDIO,
    SAPATOS,
    CASA,
    CELULARES,
    ELETRODOMESTICOS,
    COMPUTADORES,
    JOGOS,
    CAMERAS,
    SUPLEMENTOS,
)

CATEGORY_SLUGS: tuple[str, ...] = tuple(c.slug for c in CATEGORIES)

CATEGORY_BY_SLUG: dict[str, Category] = {c.slug: c for c in CATEGORIES}

# Todas as buscas do canal, já emparelhadas com a categoria que as define.
SEARCHES: tuple[tuple[Category, SearchQuery], ...] = tuple(
    (categoria, busca) for categoria in CATEGORIES for busca in categoria.searches
)


# Padrões pré-compilados por categoria.
_NICHE = {c.slug: compile_terms(c.niche_terms) for c in CATEGORIES}
_BLOCKED = {c.slug: compile_terms(c.blocked_terms) for c in CATEGORIES}
_AMBIGUOUS = {
    c.slug: tuple(
        (term, compile_term(term), tuple(compile_term(a) for a in acompanhantes))
        for term, acompanhantes in c.ambiguous_terms
    )
    for c in CATEGORIES
}


def matched_niche_term(title: str, category: Category) -> str | None:
    """Primeiro termo do nicho **desta categoria** encontrado no título.

    None significa "não é produto desta categoria".
    """
    normalizado = normalize(title)

    direto = first_match(normalizado, _NICHE[category.slug])
    if direto is not None:
        return direto

    for term, pattern, acompanhantes in _AMBIGUOUS[category.slug]:
        if pattern.search(normalizado) and any(a.search(normalizado) for a in acompanhantes):
            return term

    return None


def blocked_term(title: str, category: Category | None = None) -> str | None:
    """Primeiro termo proibido no título, olhando o canal e depois a categoria."""
    global_ = globally_blocked_term(title)
    if global_ is not None:
        return global_

    if category is None:
        return None

    return first_match(normalize(title), _BLOCKED[category.slug])


# ==========================================================================
# Template do card (HTML 1080x1920)
# ==========================================================================

TEMPLATE_HTML = r'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=1080">
<title>{{titulo}}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@500;700&display=swap" rel="stylesheet">
<style>
  /*
    Card "Achadinhos da Shopee" — 1080 x 1920 px (9:16)

    Variáveis preenchidas pelo gerar_cards.py:
      {{titulo}}   nome do produto (já encurtado)
      {{preco}}    preço atual formatado, ex.: R$ 15,13
      {{preco_antigo}}  preço antes do desconto, ex.: R$ 21,31
      {{desconto}} percentual de desconto, ex.: 29% OFF
      {{desconto_oculto}}  "oculto" quando não há desconto (esconde a linha), senão vazio
      {{nota}}     nota formatada, ex.: 4,8
      {{vendas}}   vendas formatadas, ex.: 21.353
      {{imagem}}   URL ou caminho da foto do produto
    Também disponíveis, se quiser usar no layout:
      {{loja}} {{link}} {{item_id}} {{comissao}}
  */

  :root {
    --laranja-card: #e8480a;
    --amarelo-preco: #ffe678;
    --branco: #ffffff;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  html, body {
    width: 1080px;
    height: 1920px;
    overflow: hidden;
  }

  body {
    position: relative;
    font-family: "Poppins", sans-serif;
    color: var(--branco);
    /* degradê de reserva + textura (carrinhos e etiquetas) definida no fim do arquivo */
    background-color: #f47f22;
    background-image: var(--img-fundo), linear-gradient(180deg, #fe9f33 0%, #e65f0e 100%);
    background-size: 1080px 1920px;
    background-repeat: no-repeat;
    -webkit-font-smoothing: antialiased;
  }

  /* ---------- Cabeçalho ---------- */
  .cabecalho {
    position: absolute;
    left: 65px;
    top: 65px;
    width: 950px;
    height: 210px;
    border-radius: 50px;
    background: #000;
  }

  .cabecalho__logo {
    position: absolute;
    left: 39px;
    top: 39px;
    width: 134px;
    height: 134px;
    background: var(--img-logo) center / 134px 134px no-repeat;
  }

  .cabecalho__titulo {
    position: absolute;
    left: 207px;
    top: 62px;
    font-size: 56px;
    font-weight: 700;
    line-height: 1.4;
    letter-spacing: 0.15px;
    white-space: nowrap;
  }

  /* ---------- Card do produto ---------- */
  .card {
    position: absolute;
    left: 65px;
    top: 326px;
    width: 950px;
    height: 1529px;
    border-radius: 60px;
    background: var(--laranja-card);
  }

  .card__foto {
    position: absolute;
    left: 30px;
    top: 30px;
    width: 890px;
    height: 719px;
    border-radius: 40px;
    background: var(--branco);
    /* meio pixel de suavização na borda direita/inferior, como no original */
    box-shadow: 1px 1px 0 0 rgba(255, 255, 255, 0.55);
    overflow: hidden;
  }

  .card__foto img {
    display: block;
    width: 100%;
    height: 100%;
    object-fit: contain;
  }

  /* textos: todos centralizados na largura do card */
  .card__titulo,
  .card__chamada,
  .card__preco,
  .card__info {
    position: absolute;
    left: 0;
    width: 100%;
    text-align: center;
  }

  /* área fixa de 3 linhas: títulos menores ficam centralizados nela */
  .card__titulo {
    top: 790px;
    height: 204px;
    padding: 0 95px;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .card__titulo span {
    font-size: 48px;
    font-weight: 700;
    line-height: 68px;
    display: -webkit-box;
    -webkit-box-orient: vertical;
    -webkit-line-clamp: 3;
    overflow: hidden;
  }

  .card__chamada {
    top: 1005px;
    font-size: 34px;
    font-weight: 500;
    line-height: 1.4;
  }

  .card__preco {
    top: 1036px;
    font-size: 120px;
    font-weight: 700;
    line-height: 1.4;
    color: var(--amarelo-preco);
    white-space: nowrap;
  }

  /* linha do valor antigo (riscado) + selo de desconto */
  .card__desconto {
    position: absolute;
    left: 0;
    top: 1204px;
    width: 100%;
    height: 64px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 26px;
  }

  .card__desconto.oculto { display: none; }

  .card__antigo {
    font-size: 40px;
    font-weight: 500;
    line-height: 1;
    white-space: nowrap;
    color: rgba(255, 255, 255, 0.78);
    text-decoration: line-through;
    text-decoration-thickness: 3px;
  }

  .card__selo {
    padding: 8px 24px;
    border-radius: 999px;
    background: var(--amarelo-preco);
    color: var(--laranja-card);
    font-size: 34px;
    font-weight: 700;
    line-height: 1.2;
    white-space: nowrap;
  }

  .card__info {
    top: 1300px;
    font-size: 34px;
    font-weight: 500;
    line-height: 1.4;
    white-space: nowrap;
  }

  .card__info .separador { margin: 0 0.54em; }
</style>
</head>
<body>

  <header class="cabecalho">
    <div class="cabecalho__logo" role="img" aria-label="achadinhos shopee"></div>
    <h1 class="cabecalho__titulo">Achadinhos da Shopee</h1>
  </header>

  <main class="card">
    <div class="card__foto">
      <img src="{{imagem}}" alt="{{titulo}}">
    </div>

    <h2 class="card__titulo"><span>{{titulo}}</span></h2>
    <p class="card__chamada">por apenas</p>
    <p class="card__preco">{{preco}}</p>
    <div class="card__desconto {{desconto_oculto}}">
      <span class="card__antigo">{{preco_antigo}}</span>
      <span class="card__selo">{{desconto}}</span>
    </div>
    <p class="card__info">Nota {{nota}}<span class="separador">•</span>{{vendas}} vendidos</p>
  </main>

<!-- Imagens embutidas (textura do fundo e logo), para a página funcionar num arquivo só -->
<style>
  :root {
    --img-logo: url("data:image/png;base64,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");
    --img-fundo: url("data:image/webp;base64,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");
  }
</style>
</body>
</html>
'''


# ==========================================================================
# Cards: preenchimento do template e fotografia
# ==========================================================================

LARGURA, ALTURA = 1080, 1920
EXTENSOES_FOTO = (".jpg", ".jpeg", ".png", ".webp")

# palavras que não devem ficar "penduradas" no fim de um título cortado
PALAVRAS_SOLTAS = {"de", "da", "do", "das", "dos", "com", "para", "e", "a", "o",
                   "à", "em", "c", "p", "|", "-", "sem", "por"}


# --------------------------------------------------------------------------
# Normalização do produto
# --------------------------------------------------------------------------

def _pegar(p: dict, *nomes, padrao=""):
    """Busca um campo ignorando maiúsculas/acentos comuns nos nomes."""
    chaves = {_sem_acento(k): v for k, v in p.items()}
    for nome in nomes:
        valor = chaves.get(_sem_acento(nome))
        if valor not in (None, ""):
            return valor
    return padrao


def _sem_acento(s: str) -> str:
    troca = str.maketrans("áàâãéêíóôõúç", "aaaaeeiooouc")
    return re.sub(r"[^a-z0-9]", "", str(s).lower().translate(troca))


def _numero(valor) -> float | None:
    """'R$ 15.13 (faixa ...)' -> 15.13 | '29%' -> 29.0 | 4.8 -> 4.8"""
    if isinstance(valor, (int, float)):
        return float(valor)
    achado = re.search(r"-?\d+(?:[.,]\d+)?", str(valor).replace("R$", ""))
    return float(achado.group().replace(",", ".")) if achado else None


def _normalizar(p: dict, indice: int) -> dict:
    """Converte qualquer um dos formatos de entrada num dicionário padrão."""
    loja = str(_pegar(p, "loja", "shopName", "shop_name"))
    return {
        "posicao": int(_pegar(p, "posicao", "rank", padrao=indice)),
        "item_id": str(_pegar(p, "itemId", "item_id", "id", padrao=indice)),
        "titulo": str(_pegar(p, "titulo", "productName", "nome", "name", "title")),
        "titulo_card": str(_pegar(p, "titulo_card", "titulo_curto")),  # opcional
        "preco": _numero(_pegar(p, "preco", "priceMin", "price", padrao=0)) or 0.0,
        "desconto": _numero(_pegar(p, "desconto", "priceDiscountRate", "discount", padrao=0)) or 0.0,
        "nota": _numero(_pegar(p, "nota", "ratingStar", "rating", padrao=0)) or 0.0,
        "vendas": int(_numero(_pegar(p, "vendas", "sales", "sold", padrao=0)) or 0),
        "loja": re.sub(r"\s*\(\d+\)\s*$", "", loja),
        "comissao": _numero(_pegar(p, "comissao", "commissionRate", padrao=0)) or 0.0,
        "imagem": str(_pegar(p, "imagem", "imageUrl", "image")),
        "link": str(_pegar(p, "link", "offerLink", "productLink", "url")),
    }


# --------------------------------------------------------------------------
# Formatação
# --------------------------------------------------------------------------

def formatar_preco(valor: float) -> str:
    inteiro, centavos = f"{valor:,.2f}".split(".")
    return f"R$ {inteiro.replace(',', '.')},{centavos}"


def formatar_nota(valor: float) -> str:
    return f"{valor:.1f}".replace(".", ",")


def formatar_vendas(valor: int) -> str:
    return f"{valor:,}".replace(",", ".")


def preco_antigo(preco: float, desconto: float) -> float:
    """O arquivo traz só o preço atual e o % de desconto; o antigo é preco / (1 - desconto)."""
    if not 0 < desconto < 100:
        return 0.0
    return round(preco / (1 - desconto / 100), 2)


def encurtar_titulo(titulo: str, limite: int = 60) -> str:
    """Corta o título em palavra inteira, sem deixar preposição solta no fim."""
    titulo = " ".join(titulo.split())
    if len(titulo) <= limite:
        return titulo
    palavras = titulo[: limite + 1].rsplit(" ", 1)[0].split(" ")
    while len(palavras) > 1 and palavras[-1].lower().strip(",.") in PALAVRAS_SOLTAS:
        palavras.pop()
    return " ".join(palavras).rstrip(" ,.;:|-")


# --------------------------------------------------------------------------
# HTML
# --------------------------------------------------------------------------

def variaveis_do_card(produto: dict, pasta_imagens: Path | None, limite_titulo: int) -> dict:
    """Monta as variáveis {{...}} que o template recebe."""
    imagem = produto["imagem"]
    if pasta_imagens:  # foto local tem prioridade: <pasta>/<itemId>.jpg
        for ext in EXTENSOES_FOTO:
            local = pasta_imagens / f"{produto['item_id']}{ext}"
            if local.exists():
                imagem = local.resolve().as_uri()
                break

    return {
        "titulo": produto["titulo_card"] or encurtar_titulo(produto["titulo"], limite_titulo),
        "preco": formatar_preco(produto["preco"]),
        "nota": formatar_nota(produto["nota"]),
        "vendas": formatar_vendas(produto["vendas"]),
        "imagem": imagem,
        "preco_antigo": formatar_preco(preco_antigo(produto["preco"], produto["desconto"])),
        "desconto": f"{produto['desconto']:.0f}% OFF",
        "desconto_oculto": "" if 0 < produto["desconto"] < 100 else "oculto",
        "loja": produto["loja"],
        "link": produto["link"],
        "item_id": produto["item_id"],
        "comissao": f"{produto['comissao'] * 100:.0f}%",
    }


def renderizar_html(template: str, variaveis: dict) -> str:
    """Troca cada {{nome}} pelo valor (com escape de HTML)."""
    def trocar(m: re.Match) -> str:
        nome = m.group(1)
        if nome not in variaveis:
            return m.group(0)
        return html.escape(str(variaveis[nome]), quote=True)
    return re.sub(r"\{\{\s*(\w+)\s*\}\}", trocar, template)


# --------------------------------------------------------------------------
# Imagem (screenshot)
# --------------------------------------------------------------------------

def fotografar(arquivos: list[tuple[Path, Path]]) -> list[str]:
    """Abre cada HTML no Chromium e salva o PNG. Devolve avisos, se houver."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit("Falta o Playwright:\n    pip install playwright\n    playwright install chromium")

    avisos = []
    with sync_playwright() as pw:
        navegador = pw.chromium.launch()
        pagina = navegador.new_page(
            viewport={"width": LARGURA, "height": ALTURA}, device_scale_factor=1
        )
        for arquivo_html, arquivo_png in arquivos:
            pagina.goto(arquivo_html.resolve().as_uri(), wait_until="load", timeout=60_000)
            pagina.evaluate("document.fonts.ready")
            foto_ok, fonte_ok = pagina.evaluate(
                """() => {
                    const img = document.querySelector('.card__foto img');
                    return [img.complete && img.naturalWidth > 0,
                            document.fonts.check('700 48px Poppins')];
                }"""
            )
            if not foto_ok:
                avisos.append(f"{arquivo_png.name}: a foto do produto não carregou")
                # esconde o ícone de imagem quebrada, deixando o quadro branco
                pagina.add_style_tag(content=".card__foto img{visibility:hidden}")
            if not fonte_ok:
                avisos.append(f"{arquivo_png.name}: fonte Poppins indisponível (usada fonte padrão)")
            pagina.screenshot(
                path=str(arquivo_png),
                clip={"x": 0, "y": 0, "width": LARGURA, "height": ALTURA},
            )
        navegador.close()
    return avisos


# --------------------------------------------------------------------------
# Programa principal
# --------------------------------------------------------------------------

def gerar_cards(
    produtos: list[dict],
    saida: Path,
    *,
    template: str = TEMPLATE_HTML,
    imagens: Path | None = None,
    apenas: list[int] | None = None,
    limite_titulo: int = 60,
) -> None:
    """Gera um PNG por produto. O HTML é só intermediário e fica numa pasta temporária."""
    if apenas:
        produtos = [p for p in produtos if p["posicao"] in apenas]
    if not produtos:
        sys.exit("Nenhum produto encontrado no arquivo.")

    pasta_png = saida / "png"
    pasta_png.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="cards_html_") as tmp:
        pasta_html = Path(tmp)
        arquivos = []
        for produto in produtos:
            nome = f"{produto['posicao']:02d}_{produto['item_id']}"
            variaveis = variaveis_do_card(produto, imagens, limite_titulo)
            arquivo_html = pasta_html / f"{nome}.html"
            arquivo_html.write_text(renderizar_html(template, variaveis), encoding="utf-8")
            arquivos.append((arquivo_html, pasta_png / f"{nome}.png"))
            print(f"[card] {nome}  |  {variaveis['titulo']}  |  {variaveis['preco']}")

        avisos = fotografar(arquivos)

    for _, arquivo_png in arquivos:
        print(f"[png]  {arquivo_png}")
    for aviso in avisos:
        print(f"[aviso] {aviso}", file=sys.stderr)


# ==========================================================================
# Configuração (.env)
# ==========================================================================

_CHAVES_ENV = ("SHOPEE_APP_ID", "SHOPEE_APP_SECRET", "SHOPEE_API_URL")


def ler_env() -> dict[str, str]:
    """Lê só as chaves da Shopee do `.env`; variável de ambiente tem precedência."""
    valores: dict[str, str] = {}
    candidatos = (Path(__file__).resolve().parent / ".env", Path.cwd() / ".env")
    for arquivo in dict.fromkeys(candidatos):
        if not arquivo.exists():
            continue
        for linha in arquivo.read_text(encoding="utf-8-sig").splitlines():
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, _, valor = linha.partition("=")
            if chave.strip() in _CHAVES_ENV:
                valores[chave.strip()] = valor.strip().strip('"').strip("'")
    for chave in _CHAVES_ENV:
        if os.environ.get(chave):
            valores[chave] = os.environ[chave]
    return valores


# ==========================================================================
# Cliente da Shopee Affiliate Open API (GraphQL)
# ==========================================================================

_NODE_FIELDS = """
      itemId
      productName
      price
      priceMin
      priceMax
      priceDiscountRate
      imageUrl
      offerLink
      productLink
      shopName
      shopId
      ratingStar
      sales
      commissionRate
"""

PRODUCT_OFFER_V2 = """
query productOfferV2($keyword: String, $page: Int, $limit: Int, $sortType: Int) {
  productOfferV2(keyword: $keyword, page: $page, limit: $limit, sortType: $sortType) {
    nodes {__FIELDS__}
    pageInfo {
      page
      limit
      hasNextPage
    }
  }
}
""".replace("__FIELDS__", _NODE_FIELDS)


class ShopeeAPIError(Exception):
    """Falha que não adianta repetir: 4xx, erro de GraphQL ou resposta ilegível."""


class RetryableError(Exception):
    """Falha transitória (rede, 429, 5xx). `retry_after` respeita o pedido do servidor."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def build_authorization(app_id: str, app_secret: str, timestamp: int, payload: str) -> str:
    """SHA256 de `app_id + timestamp + payload + secret`, com o payload exatamente como enviado."""
    base = f"{app_id}{timestamp}{payload}{app_secret}"
    assinatura = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return f"SHA256 Credential={app_id}, Timestamp={timestamp}, Signature={assinatura}"


async def _executar(
    http: httpx.AsyncClient,
    endpoint: str,
    app_id: str,
    app_secret: str,
    variaveis: dict[str, Any],
) -> dict[str, Any]:
    # separators sem espaço: o corpo enviado precisa ser byte a byte o assinado.
    payload = json.dumps(
        {"query": PRODUCT_OFFER_V2, "variables": variaveis},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    timestamp = int(time.time())
    try:
        resposta = await http.post(
            endpoint,
            content=payload.encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": build_authorization(app_id, app_secret, timestamp, payload),
            },
        )
    except httpx.HTTPError as exc:
        raise RetryableError(f"falha de rede: {exc}") from exc

    if resposta.status_code == httpx.codes.TOO_MANY_REQUESTS:
        try:
            espera: float | None = float(resposta.headers.get("Retry-After", ""))
        except ValueError:
            espera = None
        raise RetryableError("429 na API da Shopee", retry_after=espera)
    if resposta.status_code >= httpx.codes.INTERNAL_SERVER_ERROR:
        raise RetryableError(f"{resposta.status_code} na API da Shopee")
    if resposta.status_code >= httpx.codes.BAD_REQUEST:
        raise ShopeeAPIError(f"{resposta.status_code}: {resposta.text[:300]}")

    try:
        corpo = resposta.json()
    except ValueError as exc:
        raise ShopeeAPIError(f"resposta não é JSON: {resposta.text[:300]}") from exc
    # GraphQL devolve erro com HTTP 200; ignorar faria a página parecer vazia.
    if corpo.get("errors"):
        raise ShopeeAPIError(f"erro graphql: {json.dumps(corpo['errors'])[:300]}")
    dados = corpo.get("data")
    if not isinstance(dados, dict):
        raise ShopeeAPIError(f"resposta sem 'data': {resposta.text[:300]}")
    return dados


async def _com_retry(operacao, *, tentativas: int = 3) -> dict[str, Any]:
    """Backoff exponencial com jitter, só para falha transitória."""
    for tentativa in range(1, tentativas + 1):
        try:
            return await operacao()
        except RetryableError as exc:
            if tentativa == tentativas:
                raise
            if exc.retry_after is not None:
                espera = min(exc.retry_after, 30.0)
            else:
                espera = min(0.5 * 2 ** (tentativa - 1), 30.0) * random.uniform(0.5, 1.0)
            print(
                f"[retry] {exc} — tentativa {tentativa}/{tentativas}, espera {espera:.1f}s",
                file=sys.stderr,
            )
            await asyncio.sleep(espera)
    raise AssertionError("inalcançável")


async def buscar_ofertas(
    http: httpx.AsyncClient, env: dict[str, str], busca: str
) -> list[dict[str, Any]]:
    """Percorre até `MAX_PAGES` páginas de `PAGE_SIZE`, ordenadas por vendas."""
    endpoint = env.get("SHOPEE_API_URL") or ENDPOINT_PADRAO
    coletados: list[dict[str, Any]] = []
    for pagina in range(1, MAX_PAGES + 1):
        variaveis = {
            "keyword": busca,
            "page": pagina,
            "limit": PAGE_SIZE,
            "sortType": SORT_BY_SALES,
        }
        dados = await _com_retry(
            lambda: _executar(
                http, endpoint, env["SHOPEE_APP_ID"], env["SHOPEE_APP_SECRET"], variaveis
            )
        )
        bloco = dados.get("productOfferV2") or {}
        nodes = bloco.get("nodes") or []
        if not isinstance(nodes, list):
            raise ShopeeAPIError(f"'nodes' não é lista: {type(nodes).__name__}")
        coletados.extend(nodes)
        if not (bloco.get("pageInfo") or {}).get("hasNextPage") or not nodes:
            break
    return coletados


# ==========================================================================
# Coleta: filtra pelo nicho e fica com os 10 mais vendidos
# ==========================================================================


def categoria_por_slug(slug: str) -> Category:
    categoria = CATEGORY_BY_SLUG.get(slug)
    if categoria is None:
        raise ValueError(f"categoria desconhecida: {slug!r}. Opções: {list(CATEGORY_BY_SLUG)}")
    return categoria


def termo_de_busca(categoria: Category, produto: str | None) -> str:
    """O produto informado, ou a primeira busca da categoria na falta dele."""
    if produto and produto.strip():
        return produto.strip()
    return categoria.searches[0].keyword


def filtrar_e_ordenar(nodes: list[dict[str, Any]], categoria: Category) -> list[dict[str, Any]]:
    """Mantém o que casa com o nicho e não cai na lista negativa; ordena por vendas."""
    do_nicho = [
        n
        for n in nodes
        if matched_niche_term(str(n.get("productName", "")), categoria) is not None
        and blocked_term(str(n.get("productName", "")), categoria) is None
    ]
    return sorted(do_nicho, key=lambda n: _numero(n.get("sales") or 0) or 0, reverse=True)


def pasta_saida(base: Path, slug: str, busca: str) -> Path:
    """`<base>/<categoria>_<busca>` sem caracteres que o Windows recusa."""
    seguro = re.sub(r'[<>:"/\\|?*\x00-\x1f\s]+', "_", busca).strip("_. ")
    return base / f"{slug}_{seguro}"


# ==========================================================================
# Programa principal
# ==========================================================================


async def executar(categoria: Category, produto: str | None, saida: Path) -> None:
    env = ler_env()
    if not env.get("SHOPEE_APP_ID") or not env.get("SHOPEE_APP_SECRET"):
        sys.exit("SHOPEE_APP_ID ou SHOPEE_APP_SECRET ausentes no .env")

    busca = termo_de_busca(categoria, produto)
    print(f"[coleta] '{busca}' na categoria {categoria.slug}")
    async with httpx.AsyncClient(timeout=30) as http:
        nodes = await buscar_ofertas(http, env, busca)

    aprovados = filtrar_e_ordenar(nodes, categoria)
    escolhidos = aprovados[:LIMITE]
    print(f"[coleta] recebidos {len(nodes)} | do nicho {len(aprovados)} | usados {len(escolhidos)}")
    if not escolhidos:
        sys.exit("Nenhum produto do nicho encontrado para essa busca.")

    destino = pasta_saida(saida, categoria.slug, busca)
    destino.mkdir(parents=True, exist_ok=True)
    (destino / "produtos.json").write_text(
        json.dumps(escolhidos, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    produtos = [_normalizar(n, i) for i, n in enumerate(escolhidos, start=1)]
    # O Playwright síncrono não roda dentro do loop do asyncio: vai para outra thread.
    await asyncio.to_thread(gerar_cards, produtos, destino)


def main() -> None:
    ap = argparse.ArgumentParser(description="Coleta 10 produtos na Shopee e gera os cards.")
    ap.add_argument("categoria", nargs="?", help="slug da categoria")
    ap.add_argument("produto", nargs="?", default=None, help="termo de busca (opcional)")
    ap.add_argument("--saida", type=Path, default=Path("saida"), help="pasta base de saída")
    ap.add_argument("--categorias", action="store_true", help="lista os slugs e sai")
    args = ap.parse_args()

    if args.categorias:
        for c in CATEGORIES:
            print(f"{c.slug:18} {c.label}")
        return
    if not args.categoria:
        ap.error("informe a categoria (ou use --categorias)")
    try:
        categoria = categoria_por_slug(args.categoria.strip())
    except ValueError as erro:
        sys.exit(str(erro))

    asyncio.run(executar(categoria, args.produto, args.saida))


if __name__ == "__main__":
    main()
