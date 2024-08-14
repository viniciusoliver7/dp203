--==============INSERÇÃO DOS DADOS======================
    SELECT 
    * 
    INTO [MinhaTabelaDest]
    FROM [MinhaOrigemHUb] 


-- ==================EXTRAÇÃO DE MATRIZ =======================================
    /*
        [
        {resorce:[
            {table:"minhatabela",
            ano:2001-30-09}
        ]
        
        nome:"algo"}
        ]
    */

    SELECT 
    Resource.ArrayValue("table") as table,
    Resource.ArrayValue("ano") as data,
    i.nome as nome
    INTO [MeuDestino] 
    FROM [MinhaOrigem] i
    CROOS APPLY GetArrayArguments(i.resorce) as Resource

-- ================== usando a Tumbling Window=======================================
    with column AS 
    (
        SELECT * from meusdados
    )

    SELECT COUNT(meusdados),coluna1, coluna2
    INTO column
    from [MeuDestino]
    group BY coluna1,coluna,TumblingWindow(minute,2)

--==================== STREAM COM VARIAS SAIDASS ==============================

    /*
        ele funciona como uma espécie de arquivo sql que 
        pode excultar varias queryes seguidamente
    */


    select * 
    into [MeuDestino]
    from  [MinhaOrigem]

    select * 
    into [MeuDestino2]
    from  [MinhaOrigem2]


-- ===================== DESCOMPACTANDO  MATRIX =================================
        /*
    [
        {
            "usuario": "teste@123",
            "caractaristicas": [
                {
                    "nome": "vinicius",
                    "nascionalidade": [
                        {
                            "cidade": "sao paulo",
                            "pais": "Brasil"
                        }
                    ]
                }
            ]
        }
    ]

        */


    with step1 as (
    SELECT
        h.usuario,
        carac.ArrayValue.nome as nome,
        carac.ArrayValue.nascionalidade as nascionalidade
    FROM
        [hubjson] h
    CROSS APPLY GetArrayElements(h.caractaristicas) as carac
    ),step2 
    as (
    SELECT
    usuario,
    nome,
    GetArrayElement(nascionalidade,0) nascionalidade
    from step1 
    ), step3 as (
    select 
    usuario,
    nome,
    nascionalidade.cidade  as cidade , 
    nascionalidade.pais as pais 
    from step2
    )      

    select * 
    into [meubancodedados450]
    from step3