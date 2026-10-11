# Gerador de Cards de Achadinhos

Este programa procura na Shopee os produtos mais vendidos de um assunto (por exemplo, "fone bluetooth") e cria uma imagem (PNG) pronta para postar de cada um.

Em resumo: você digita a **categoria** e o **produto**, e ele devolve uma pasta com as imagens.

---

## Passo a passo completo

### Passo 1 — Instalar o Python

1. Acesse https://www.python.org/downloads/ e baixe a versão mais recente (3.10 ou superior).
2. Abra o instalador e **marque a opção "Add Python to PATH"** antes de clicar em *Install Now*.
3. Confirme que deu certo: abra o **PowerShell** (tecle Windows, digite "PowerShell" e Enter) e rode:

   ```
   python --version
   ```

   Deve aparecer algo como `Python 3.11.0` ou maior. Se aparecer erro de "comando não reconhecido", reinstale o Python marcando a opção do PATH.

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

### Passo 5 — Ver as categorias disponíveis

```
python pipeline_cards.py --categorias
```

Categorias atuais: `audio`, `sapatos`, `casa`, `celulares`, `eletrodomesticos`, `computadores`, `jogos`, `cameras` e `suplementos`.

### Passo 6 — Gerar os cards

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

### Passo 7 — Pegar as imagens

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

