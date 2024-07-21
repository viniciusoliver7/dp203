
-- ==========================================
-- CREATE EXTERNAL TABLE POOL SQL SERVELESS csv
-- ==========================================
--o



    CREATE DATABASE [testePokemon1];

    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Teste&&@'

    CREATE DATABASE SCOPED CREDENTIAL SasToken
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE' , 
    SECRET = 'sas token'

    CREATE EXTERNAL DATA SOURCE datapokemon
    with (
    LOCATION ='https://testearmazenamento25045.blob.core.windows.net/teste'
    ,CREDENTIAL=SasToken
    )

    CREATE EXTERNAL FILE FORMAT arquivoPokemon WITH (
    FORMAT_TYPE = DELIMITEDTEXT ,
    FORMAT_OPTIONS(
    FIELD_TERMINATOR = ',',
    FIRST_ROW = 2
    ))

    CREATE EXTERNAL TABLE pokedex0(
    [dexnum] [varchar](2000) NULL, 
    [name] [varchar](2000) NULL,
    generation VARCHAR(2000) NULL,
    type1 VARCHAR(2000) NULL , 
    type2 VARCHAR(2000) NULL,
    species VARCHAR(2000) NULL,
    height VARCHAR(2000) NULL,
    weight VARCHAR(2000) NULL,
    ability1 VARCHAR(2000) NULL,
    ability2 VARCHAR(2000) NULL,
    hidden_ability VARCHAR(2000) NULL, 
    hp VARCHAR(2000) NULL,
    attack VARCHAR(2000) NULL,
    defense VARCHAR(2000) NULL,
    sp_atk VARCHAR(2000) NULL,
    sp_def VARCHAR(2000) NULL,
    speed VARCHAR(2000) NULL,
    total VARCHAR(2000) NULL,
    ev_yield VARCHAR(2000) NULL,
    catch_rate VARCHAR(2000) NULL,
    base_friendship VARCHAR(2000) NULL,
    base_exp VARCHAR(2000) NULL,
    growth_rate VARCHAR(2000) NULL,
    egg_group1 VARCHAR(2000) NULL,
    egg_group2 VARCHAR(2000) NULL,
    percent_male VARCHAR(2000) NULL,
    percent_female VARCHAR(2000) NULL,
    gg_cycles VARCHAR(2000) NULL,
    special_group VARCHAR(2000) NULL
    )
    WITH (
    LOCATION = '/pokemon_data.csv',
    DATA_SOURCE=datapokemon,
    FILE_FORMAT =arquivoPokemon
    )




-- ==========================================
-- USAR O OPENROWSET
-- ==========================================
    select * from OPENROWSET(
        bulk '/pokemon_data.csv',
        FORMAT ='csv',
        DATA_SOURCE='dataPokemon',
        FIELDTERMINATOR=',',
        parser_version = '2.0',
        FIRSTROW=2
    ) as [file]


-- ==========================================
-- CREATE EXTERNAL TABLE POOL SQL SERVELESS json
-- ==========================================
    select top 10 *
    from openrowset(
            bulk 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.jsonl',
            format = 'csv',
            fieldterminator ='0x0b',
            fieldquote = '0x0b'
        ) with (doc nvarchar(max)) as rows
    go
    select top 10 *
    from openrowset(
            bulk 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.json',
            format = 'csv',
            fieldterminator ='0x0b',
            fieldquote = '0x0b',
            rowterminator = '0x0b' --> You need to override rowterminator to read classic JSON
        ) with (doc nvarchar(max)) as rows




-- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
-- CREATE EXTERNAL TABLE SQL POOL DEDICATE csv
-- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '10825@hpop'


    CREATE DATABASE SCOPED CREDENTIAL pokekey 
    with IDENTITY = 'pokemonblob2045'
    ,SECRET='key acces'


    CREATE EXTERNAL  DATA SOURCE pokedata WITH
    (
        LOCATION='abfss://pokedirectory@pokemonblob2045.dfs.core.windows.net',
        CREDENTIAL = pokekey,
        TYPE = HADOOP
    )
    drop EXTERNAL FILE FORMAT pokefile

    CREATE EXTERNAL FILE FORMAT pokefile
    with(
        FORMAT_TYPE=DELIMITEDTEXT,
    FORMAT_OPTIONS (             
            FIELD_TERMINATOR =',', 
            STRING_DELIMITER = '"' ,
            First_Row = 2,
            Encoding = 'UTF8'
        )
    )


    CREATE external table pokemon (
    dexnum varchar(400) NULL,
    name varchar(400) NULL,
    generation	varchar(400) NULL,
    type1	varchar(400) NULL,
    type2	varchar(400) NULL,
    species	varchar(400) NULL,
    height	varchar(400) NULL,
    weight	varchar(400) NULL,
    ability1	varchar(400) NULL,
    ability2	varchar(400) NULL,
    hidden_ability	varchar(400) NULL,
    hp	varchar(400) NULL,
    attack	varchar(400) NULL,
    defense	varchar(400) NULL,
    sp_atk	varchar(400) NULL,
    sp_def	varchar(400) NULL,
    speed	varchar(400) NULL,
    total	varchar(400) NULL,
    ev_yield	varchar(400) NULL,
    catch_rate	varchar(400) NULL,
    base_friendship	varchar(400) NULL,
    base_exp varchar(400) NULL,
    growth_rate	varchar(400) NULL,
    egg_group1	varchar(400) NULL,
    egg_group2	varchar(400) NULL,
    percent_male	varchar(400) NULL,
    percent_female	varchar(400) NULL,
    egg_cycles	varchar(400) NULL,
    special_group varchar(400) NULL

    )
    with (
        LOCATION='/pokemon_data.csv',
        DATA_SOURCE=pokedata,
        FILE_FORMAT=pokefile
    )


    SELECT * from pokemon


--====================================================
--COMOMADNDO PARA CRIAR TABELA POLYBASE
--===================================================
    CREATE TABLE pokedexpoly
    with(
        DISTRIBUTION=ROUND_ROBIN
    ) as 
    SELECT * FROM pokemon



-- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
-- COMOMADNDO PARA CRIAR TABELA COPY | CRIAR TABELA FISICA E COPIAR DADOS DO ARQUIVO CSV
-- =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


    --cria uma tabela
    CREATE table pokemonfix (
    dexnum varchar(400) NULL,
    name varchar(400) NULL,
    generation	varchar(400) NULL,
    type1	varchar(400) NULL,
    type2	varchar(400) NULL,
    species	varchar(400) NULL,
    height	varchar(400) NULL,
    weight	varchar(400) NULL,
    ability1	varchar(400) NULL,
    ability2	varchar(400) NULL,
    hidden_ability	varchar(400) NULL,
    hp	varchar(400) NULL,
    attack	varchar(400) NULL,
    defense	varchar(400) NULL,
    sp_atk	varchar(400) NULL,
    sp_def	varchar(400) NULL,
    speed	varchar(400) NULL,
    total	varchar(400) NULL,
    ev_yield	varchar(400) NULL,
    catch_rate	varchar(400) NULL,
    base_friendship	varchar(400) NULL,
    base_exp varchar(400) NULL,
    growth_rate	varchar(400) NULL,
    egg_group1	varchar(400) NULL,
    egg_group2	varchar(400) NULL,
    percent_male	varchar(400) NULL,
    percent_female	varchar(400) NULL,
    egg_cycles	varchar(400) NULL,
    special_group varchar(400) NULL

    )


    --copula a tabela com os dados do arquivo

    COPY INTO pokemonfix FROM '[URL OBJETO DATALAKE]'
    WITH(
        FILE_TYPE='CSV', -- pode trocar o csv para parquet que cria tbm 
        FIRSTROW=2,
        CREDENTIAL=(IDENTITY='Storage Account Key',SECRET='[COLOCAR CHAVE DATALAKE]')
    )



--====================================================
--COMANDO DE SELECT PARA OBTER INFORMAÇÕES
--===================================================
    select  * from sys.external_file_formats -- MOSTRA OS FILE FORMATS EXISTENTES

    SELECT * from sys.external_data_sources -- MOSTRA OS DATA SOURCES EXISTENTES 

    SELECT * from sys.database_scoped_credentials -- MOSTRA AS CREDENCIAIS DE DATABASE EXISTENTES 

    select * from  sys.external_tables -- MOSTRA AS EXTERNAL TABLES EXISTENTES




--====================================================
--criando uma dimenção
--=====================================================

    --criea uma view com o dado
    

    --CRIE UMA TABELA USANDO A VIEW COMO BASE  
    SELECT [COLUNAS] INTO [NOME_DA_TABELA] FROM VIEW_COM_DADOS


-- ==========================================================
-- COMANDO PARA VERIFICAR A DISTRIBUICAO DA TABELA
--====================================================
    select 
    OBJECT_NAME( OBJECT_ID ) as table_name,
    *
    from sys.pdw_table_distribution_properties

--====================================================
--COMANDO PARA CRIAR UMA TABELA COM DISTRIBUIÇÃO HASH   / round robin / REPLICADAS
--===================================================
    --HASH
        create table nome (coluna STRING)
        with(
        DISTRIBUTION=HASH(coluna)
        )

    -- ROUND ROBIN
        create table nome (coluna STRING)
        with(
            DISTRIBUTION=ROUND_ROBIN
            )


    --REPLICADAS
        create table nome (coluna STRING)
        with(DISTRIBUTION=REPLICATE)


--====================================
-- CRIAR TABELA DISTRIBUIDA EM HASH
--====================================
        create table nome (coluna1 STRING, coluna2)
        with(
        DISTRIBUTION=HASH(coluna1,coluna2)
        )





--====================================
-- criando chave substitutiva
--====================================
    create table teste (
        productSK int IDENTITY(1,1) NOT NULL,
        IDCLIENE INT NOT NULL,
        cliente varchar(200) not null
    )



--====================================================
--COMANDO PARA CRIAR UMA TABELA COM DADOS PARTICIONADOS
--===================================================
    --HASH
        create table nome (coluna STRING)
        with(
        DISTRIBUTION=HASH(coluna)
        PARTITION (
            CAMPODATA RANGE RIGHT FOR VALUES
            ('DATAINICIO','DATAFIM')
        )
        )


--====================================================
-- CRIAR PROCEDORE
--=====================================================
    CREATE PROCEDORE [TESTE]
        DELETE FROM [TABLE]


