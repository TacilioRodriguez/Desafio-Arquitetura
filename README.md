# Desafio-Arquitetura
 Desafio de Arquitetura

Construir uma DAG com o Apache Airflow que consulte a api pública https://disneyapi.dev/docs
e poste os dados a cada 5 minutos no data lake desenvolvido acima.


Ferramentas utilizadas
Docker
Airflow

Como utilizo Macbook M2 é necessário efetuar algumas alterações. Após efetuar a instalação do Docker instale o HomeBrew
defina as permissões para adicionar a variavel ao sistema e entre no modo de virtualEnv. Abaixo exemplo de como fazer.

Para criar um ambiente virtual usando o Homebrew no macOS, siga estes passos:

1. **Instale o Python 3 usando o Homebrew**, se ainda não tiver instalado:Isso instalará o Python 3.10 e o **`pip`** correspondente.
    
    `bashbrew install python@3.10`
    
2. **Crie um ambiente virtual usando o módulo `venv` do Python 3**:Esse comando cria um ambiente virtual chamado **`airflow_venv`** no diretório **`~/install/airflow-tutorial/`**.
    
    `bashpython3 -m venv ~/install/airflow-tutorial/airflow_venv`
    
3. **Ative o ambiente virtual**:Isso ativa o ambiente virtual criado anteriormente.
    
    `bashsource ~/install/airflow-tutorial/airflow_venv/bin/activate`
    
4. **Verifique se o ambiente virtual está ativo**:Deve mostrar o caminho para o Python dentro do ambiente virtual, por exemplo:
    
    `bashwhich python`
    
    `/Users/username/install/airflow-tutorial/airflow_venv/bin/python`


Feito isso é possivel efetuar instalações de bibliotecas que serão utilizadas no desenvolvimeneto.

Para montar um docker crie uma pasta em qualquer lugar do seu Mac, abra um VSCode e aponte o VSCode para essa pasta e crie um arquivo docker-compose.yaml

Copie o conteudo desse link (https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml) e cole no arquivo que acabamos de criar.

Feito isso, crie outras 4 pastas sendo elas:
- config
- dags
- logs
- plugins

Depois de todo esse processo, abra o terminal pelo VSCode e selecione bash e execute os comandos nessa ordem:

- docker-compose up airflow-init
- docker-compose up

Aguarde de cada um dos comandos acima finalizar q seu conteiner vai subir com a imagem do Airflow.

Obs.: no arquivo xx na parte de environment insira essa linha para que o botão de testar conexão na UI do Airflow seja habilitado.

AIRFLOW__CORE__TEST__CONNECTION: Enabled



# Desenho da Arquitetura



![Arquitetura - Desafio](https://github.com/TacilioRodriguez/Desafio-Arquitetura/assets/50462656/b4891f61-d07e-4495-bac2-7bfe16d608fa)


