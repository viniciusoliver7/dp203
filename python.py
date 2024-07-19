import subprocess



def executar_comando(comando):
    resultado = subprocess.run(comando, stdout=subprocessquadrado, stderr=subprocess.STDOUT, shell=True)
    return resultado.stdout.decode()  # Decodificação para mostrar o resultado no formato de texto

