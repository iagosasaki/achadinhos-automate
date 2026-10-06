# Gerador de Cards de Achadinhos

Este programa procura na Shopee os 10 produtos mais vendidos de um assunto (por exemplo, "fone bluetooth") e cria uma imagem pronta para postar de cada um.

## Antes de começar (só na primeira vez)

1. Instale o **Python** (python.org/downloads). Na instalação, marque a opção **"Add Python to PATH"**.
2. Abra o **Terminal** (no Windows, procure por "PowerShell"), entre na pasta do programa e digite estas duas linhas, uma de cada vez:

   ```
   pip install httpx playwright
   playwright install chromium
   ```

3. Na mesma pasta do programa, crie um arquivo chamado `.env` com as suas chaves da Shopee:

   ```
   SHOPEE_APP_ID=sua_chave_aqui
   SHOPEE_APP_SECRET=seu_segredo_aqui
   ```

## Como usar

No Terminal, dentro da pasta do programa, digite:

```
python pipeline_cards.py audio "fone bluetooth"
```

- `audio` é a **categoria**.
- `"fone bluetooth"` é o **produto** que você quer buscar (fica entre aspas). Se não escrever, o programa usa uma busca padrão da categoria.

Para ver todas as categorias disponíveis:

```
python pipeline_cards.py --categorias
```

## Onde ficam as imagens

Dentro da pasta `saida`, o programa cria uma pasta com o nome da categoria e do produto, por exemplo `saida/audio_fone_bluetooth`. As imagens estão na subpasta `png`.

Se aparecerem menos de 10 imagens, é porque a Shopee não devolveu 10 produtos que combinassem com a categoria.
