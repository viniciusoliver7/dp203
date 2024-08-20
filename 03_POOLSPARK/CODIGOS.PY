
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



#=========================================================================