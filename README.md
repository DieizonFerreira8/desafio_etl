# Desafio ETL

# Projeto ETL em Python com Dados Públicos

Este projeto é um exemplo simples de um processo ETL (Extract, Transform, Load) usando Python e o conjunto de dados COVID-19 disponibilizado pelo Center for Systems Science and Engineering (CSSE) na Johns Hopkins University.

## Extração

A primeira etapa do processo ETL é a extração. Nesta etapa, utilizamos a biblioteca `requests` para baixar o conjunto de dados diretamente do repositório do GitHub da Johns Hopkins University. O conjunto de dados é lido em um DataFrame pandas a partir do texto da resposta HTTP.

```python
def extract():
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    req = requests.get(url)
    data = StringIO(req.text)
    df = pd.read_csv(data)
    return df
```

## Transformação

A segunda etapa do processo ETL é a transformação. Nesta etapa, transformamos o formato largo do conjunto de dados para um formato longo usando a função `melt` do pandas. Além disso, convertemos a coluna ‘Date’ para o tipo datetime.

```python
def transform(df):
    df = df.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], var_name='Date', value_name='Confirmed Cases')
    df['Date'] = pd.to_datetime(df['Date'])
    return df
```

## Carregamento

A terceira e última etapa do processo ETL é o carregamento. Nesta etapa, salvamos o DataFrame transformado em um arquivo CSV.

```python
def load(df, filename):
    df.to_csv(filename, index=False)
    print(f'Dados salvos em {filename}')
```

## Execução

Finalmente, executamos o processo ETL chamando as funções `extract`, `transform` e `load` em sequência.

```python
def run_etl():
    df = extract()
    df = transform(df)
    load(df, 'covid_data.csv')

run_etl()
```