
#=====================================LENDO ARQUIVOS DO LAKE======================================
'''
quando usamos o spark , lembrese de adicionar no blob uma função de assinatura para permitir a leitura
no blob, pesquise la apenas blob e escolha o usuario

o path vc deve ser modificado, mudando o protocoo de https -> abfss e o blob -> dfs
e coloque o conteiner mais alto na frente do nome do recurso seguido de um @ 
'''



pathOriginal='https://meuarmazemadls.blob.core.windows.net/ficheirodemacho/meusdados/pokemon.json'
pathEditado="abfss://ficheirodemacho@meuarmazemadls.dfs.core.windows.net/meusdados/pokemon.json"

df = spark.read.load(pathEditado,inferschema=True,sep=",",header=True,format="json")

display(df)



#===================================lendo arquivo json ======================================
    #{
    #"customerid":1,
    #"customername":"UserA",
    #"registered":true,
    #"courses":["AZ-900","AZ-500","AZ-303"],
    #"details" :
    #	{
    #	"mobile":"111-1112",
    #	"city":"CityA"
    #	}
    #},

df.select(col("customerid"), explode(col("courses")) ,  col("details.city")).show()


#=============================================copy Command============================

%sql
COPY INTO [tabelinha]
from "path"
FILEFORMAT = FORMATO
COPY_OPTIONS ("mergeSchema","True")


#=======================================leitura com streming de dados============================

shema2="pensa no schema"
df=spark.readStream.format("cloudFiles").option("clounFile","csv").option("schema", shema).load(path)


#===============================ESCREVENDO DADOS EM STREAMING =======================================

df.writeStream.format("delta")\
.outputMode("append")\
.option("checkpointLocation",caminhoparalog)\
.option("mergeSchema","True")\
.table("minha tabelinha")