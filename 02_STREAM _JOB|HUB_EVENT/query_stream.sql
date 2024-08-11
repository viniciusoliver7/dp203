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


-- =====================  =================================
