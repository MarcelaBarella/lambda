# ETL para o Big Table

## Ferramentas
* [Cloud Functions](https://cloud.google.com/functions/docs/console-quickstart)
  * [requests](https://requests.readthedocs.io/en/latest/)
* [Cloud pub/sub](https://cloud.google.com/pubsub/docs/overview?hl=en)
* [Cloud DataFlow](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-python)
  * [Apache Beam](https://github.com/apache/beam)
  * [Pandas](https://pandas.pydata.org/)
* [Cloud BigTable](https://cloud.google.com/bigtable/docs?hl=en)
  * [Bigtable HBase](https://github.com/googleapis/java-bigtable-hbase)
* [Google Scheduler](https://cloud.google.com/scheduler/docs/schedule-run-cron-job?hl=en)
* [Cloud Pricing Calculator](https://cloud.google.com/products/calculator)

## Descrição

### Cloud Functions
---
A primeira ferramenta usada no fluxo será a **cloud functions**, que basicamente é um serviço/ferramenta que responde a eventos, no nosso caso, uma chamada no endpoint da API que dispõe os dados das notas fiscais na forma de json, no caso para fins de exemplo decidi utilizar a biblioteca requests do python, porém é possível usar não somente outras bibliotecas como também outras linguagens para chamada deste endpoint, além do fato de que o google cloud permite maiores manipulações do json obtido.
Uma das vantagens na utiliazação do cloud functions é que se trata de uma tecnologia serverless, ou seja, preocupações com infra estrutura e afins são delegadas a própria gcp.

### Cloud Pub/Sub
---
Na proxima etapa, configuraremos um **cloud pub/sub** que acionará o gatilho da nossa cloud function quanto evento.
O cloud pub/sub nada mais é do que um serviço de menssageria que funciona de forma assíncrona, ou seja, nele criaremos *producers* (quem irá produzir os dados) e *consumers* (quem consequntemente, irá consumir os dados).
Primeiro é necessário criar um topic e em seguida um subscriber para esse topic. Sendo o subscriber do tipo push e utilizando a url de trigger do cloud function, para que a menssagem desse subscriber contenha então o json com as notas fiscais.
Caso se faça necessário, é possível também criar schemas para padronizar as notas fiscais contidas nas mensagens e facilitar o processamento delas em etapas futuras.
Este serviço pode ser utilizado também para fins de streaming, integrando com o Apache Spark (utilizado no exercício 2), que por exemplo, após o processamento dos dados, poderia escrever em um BigQuery.

### Cloud Dataflow
---
O **cloud dataflow** será usado na etapa de transformação do json contido na API.
Como mencionado acima, o dataflow é um serviço usado para enriquecer e transformar os dados que recebe. Pode ser integrado com diferentes outros serviços como Cloud Storage, Cloud Firestone, BigQuery, etc.
Após realizar as devidas configurações do pub/sub e do dataflow, como por exemplo escolher o bucket, região etc, é necessário com de escrita no BigTable, utilizando Python com a biblioteca pandas para manipulação de dados na forma de dataframes, podendo ser estes no formato csv, xlxs, json etc, assim como feito no exercício 3. Também utilizaremos o apache beam, um modelo unificado para streaming, que além de open source permite que o código de criação de pipelines seja escrito apenas uma vez e integrado com diversas tecnologias que servem de "runners", além disso, é possível utilizar diferentes linguagens de programação diferentes para o código das pipelines.

### Cloud BigTable
---
O **cloud bigtable** se trata de um serviço de armazenamento da gcp capaz de armazenar quantidades massivas de dados.
Se aplica bem ao armazenamento de grafos, internet das coisas, marketing etc.
Apesar de armazenar as informações em linhas e colunas no modelo chave/valor, não se trata de um banco relacional e não suporta queries SQL.
Para conectarmos uma pipe do cloud dataflow ao cloud bigtable e assim consequentemente realizar operações de escrita nele é necessário utilizar um conector escrito em Java baseado no Apache Beam chamado Bigtable HBase. 
Basta então escrever os códigos de configuração e escrita, das quais se pode encontrar códigos de exemplo nas documentações oficiais.

### Cloud Scheduler
---
Após a criação deste fluxo de trabalho, podemos agendar sua inicialização com o **cloud scheduler** criando cron jobs para sua execução em dias e horas especificas de uma semana por exemplo.

### Cloud Pricing Calculator
---
Depois de construida sua etl, você poderá calcular o custo total mensal desta utilizando o **cloud Pricing Calculator**, onde você dira quais serviços e quais suas especificações irá usar, como por exemplo tamanho de RAM, uso de CPU, localização dos servidores e etc,e então será gerado um relatório detalhado que você pode enviar para seus clientes e afins.

