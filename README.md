# Gerador de Cards de Achadinhos

Este programa procura na Shopee os produtos mais vendidos de um assunto (por exemplo, "fone bluetooth") e cria uma imagem (PNG) pronta para postar de cada um.

Em resumo: você digita a **categoria** e o **produto**, e ele devolve uma pasta com as imagens.

---

## Passo a passo completo

### Passo 1 — Ter uma conta de afiliado da Shopee (com as chaves da API)

O programa usa a API de afiliados da Shopee, então você precisa de duas chaves: **App ID** e **Secret**.

1. Crie ou acesse sua conta no **Programa de Afiliados da Shopee Brasil** (affiliate.shopee.com.br).
2. No painel de afiliados, abra a área de **API / Open API** e gere (ou copie) o seu **App ID** e o seu **Secret**.
3. Guarde os dois em um lugar seguro. Eles funcionam como login e senha: não compartilhe e não publique.

> Os nomes exatos dos menus podem mudar. Se não achar, procure por "Open API" ou "API de Afiliados" no painel.

### Passo 2 — Instalar o Python

1. Acesse https://www.python.org/downloads/ e baixe a versão mais recente (3.10 ou superior).
2. Abra o instalador e **marque a opção "Add Python to PATH"** antes de clicar em *Install Now*.
3. Confirme que deu certo: abra o **PowerShell** (tecle Windows, digite "PowerShell" e Enter) e rode:

   ```
   python --version
   ```

   Deve aparecer algo como `Python 3.11.0`. Se aparecer erro de "comando não reconhecido", reinstale o Python marcando a opção do PATH.

### Passo 3 — Abrir a pasta do projeto no Terminal

No PowerShell, entre na pasta onde está o programa (ajuste o caminho para o seu):

```
cd C:\Users\SeuUsuario\Documents\achadinhos-automate
```

Dica: no Explorador de Arquivos, abra a pasta, clique na barra de endereço, digite `powershell` e tecle Enter. O terminal abre já na pasta certa.

### Passo 4 — Instalar as bibliotecas

Rode as duas linhas abaixo, **uma de cada vez**, e espere cada uma terminar:

```
pip install httpx playwright
playwright install chromium
```

- A primeira instala as bibliotecas do programa (`httpx` para falar com a Shopee e `playwright` para gerar as imagens).
- A segunda baixa o navegador Chromium (algumas centenas de MB). O programa usa esse navegador "escondido" para tirar a foto de cada card.

### Passo 5 — Configurar as chaves (arquivo `.env`)

1. Na pasta do programa, faça uma cópia do arquivo `.env.example` e chame a cópia de `.env`. Pelo PowerShell:

   ```
   copy .env.example .env
   ```

2. Abra o `.env` no Bloco de Notas e preencha com as chaves do Passo 1:

   ```
   SHOPEE_APP_ID=sua_chave_aqui
   SHOPEE_APP_SECRET=seu_segredo_aqui
   ```

   - Sem espaços ao redor do `=` e sem aspas.
   - A linha `SHOPEE_API_URL` é opcional. Pode deixar como está.
3. Salve o arquivo. O `.env` já está no `.gitignore`, então não será enviado ao GitHub por engano.

### Passo 6 — Ver as categorias disponíveis

```
python pipeline_cards.py --categorias
```

Categorias atuais: `audio`, `sapatos`, `casa`, `celulares`, `eletrodomesticos`, `computadores`, `jogos`, `cameras` e `suplementos`.

### Passo 7 — Gerar os cards

```
python pipeline_cards.py audio "fone bluetooth"
```

- `audio` é a **categoria**.
- `"fone bluetooth"` é o **produto** que você quer buscar (entre aspas). Se omitir, o programa usa a primeira busca padrão da categoria:

  ```
  python pipeline_cards.py audio
  ```

Mais exemplos:

```
python pipeline_cards.py jogos "controle xbox"
python pipeline_cards.py sapatos "tênis masculino"
```

Durante a execução você verá mensagens como `[coleta]`, `[card]` e `[png]`. Quando terminar, as imagens estão prontas.

### Passo 8 — Pegar as imagens

Dentro da pasta `saida`, o programa cria uma pasta com o nome da categoria e do produto, por exemplo:

```
saida/audio_fone_bluetooth/
├── produtos.json      (dados dos produtos coletados)
└── png/
    ├── 01_40331456554.png
    ├── 02_29320805319.png
    └── ...
```

As imagens estão na subpasta `png`, numeradas pela posição de vendas (01 = mais vendido). Para escolher outra pasta de saída, use `--saida`:

```
python pipeline_cards.py audio "fone bluetooth" --saida D:\meus_cards
```

---

## Requisitos resumidos

| O quê | Para quê |
|---|---|
| Windows, macOS ou Linux | Sistema onde o programa roda |
| Python 3.10+ | Executa o programa |
| `httpx` e `playwright` (via `pip`) | Acesso à Shopee e geração das imagens |
| Chromium (via `playwright install chromium`) | "Tira a foto" de cada card |
| Conta de afiliado Shopee (App ID e Secret) | Autorizar a consulta de produtos |
| Internet ligada | Consultar a Shopee, baixar as fotos dos produtos e a fonte Poppins (Google Fonts) |

## Problemas comuns

| Mensagem ou sintoma | O que fazer |
|---|---|
| `'python' não é reconhecido` | Reinstale o Python marcando **Add Python to PATH** e abra um novo terminal. |
| `Falta o Playwright` | Rode `pip install playwright` e depois `playwright install chromium`. |
| `ModuleNotFoundError: No module named 'httpx'` | Rode `pip install httpx playwright`. |
| `SHOPEE_APP_ID ou SHOPEE_APP_SECRET ausentes no .env` | Confira se o arquivo se chama exatamente `.env` (não `.env.txt`), está na pasta do programa e tem as duas chaves preenchidas. |
| `categoria desconhecida` | Confira o nome com `python pipeline_cards.py --categorias`. |
| `Nenhum produto do nicho encontrado para essa busca` | A busca não retornou produtos da categoria escolhida. Tente outro termo ou outra categoria. |
| Erro de autenticação / acesso negado da Shopee | Chaves erradas ou sem permissão. Gere-as novamente no painel de afiliados. |
| `[aviso] a foto do produto não carregou` | Esse card ficou com o quadro da foto em branco (problema de rede ou da imagem). Rode de novo. |
| `[aviso] fonte Poppins indisponível` | Sem acesso ao Google Fonts. O card usa uma fonte padrão. Verifique a internet e rode de novo. |

## Observações

- O programa busca até os 100 mais vendidos da Shopee para o termo, filtra os que combinam com a categoria e gera cards para os **20 primeiros** (limite definido por `LIMITE` no `pipeline_cards.py`). Se aparecerem menos imagens, é porque a Shopee não devolveu produtos suficientes que combinassem com a categoria.
- Rodar de novo com a mesma categoria e produto sobrescreve os arquivos da pasta correspondente em `saida`.
- A pasta `saida` e o arquivo `.env` não vão para o Git (estão no `.gitignore`).
