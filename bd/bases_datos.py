import mysql.connector
import os 
import subprocess
import datetime

conexion = { "host" : "localhost",
             "user" : "root",
             "password" : "mendez10",
}

carpeta_principal = os.path.dirname(__file__)
#print(carpeta_principal)

carpeta_respaldo = os.path.join(carpeta_principal, "respaldo")
#print(carpeta_respaldo)

if not os.path.exists(carpeta_respaldo):
    os.makedirs(carpeta_respaldo)
    
class BasesDatos:
    def __init__(self, **kwargs):
        self.conector = mysql.connector.connect(**kwargs)
        self.cursor = self.conector.cursor()
        self.host = kwargs["host"]
        self.usuario = kwargs["user"]
        self.contraseña = kwargs["password"]
        self.conexion_cerrada = False
        
        #avisa que se abrio la conexion con el servidor
        print("Se abrio la conexion con el servidor")
        
    #decorador para el informe de las bases de datos
    def informe_bd (funcion_parametro):
       def interno (self, *args):
           funcion_parametro(self, *args)
           BasesDatos.mostrar_bd(self)
       return interno
   
   #decorador de cierre.
    def conexion (funcion_parametro): 
        def interno (self, *args, **kwargs):
            try: 
                if self.conexion_cerrada:
                    self.conector = mysql.connector.connect(
                        host = self.host, 
                        user = self.uuario,
                        password = self.contraseña
                    )
                    
                    self.cursor = self.conector.cursor()
                    self.conexion_cerrada = False
                    print("Se abrio la conexion con el servidor")
                funcion_parametro(self, *args, **kwargs)  
            except: 
                print("Ocurrio un error de llamada")    
            finally: 
                if self.conexion_cerrada:
                    pass
                else: 
                  self.cursor.close()
                  self.conector.close()
                  print("Conexion cerrada con el servidor")
                  self.conexion_cerrada = True
        return interno

    #consulta bases datos 
    @conexion
    def consulta (self, sql):
        try: 
           self.cursor.execute(sql)
           print("Esta es la salida de la instruccion que has puesto")  
           print(self.cursor.fetchall())          
        except: 
            print("Error la instruccion sql no es correcta")
      
    #mostrar bases de datos 
    @conexion
    def mostrar_bd (self):
        try:
            # Se informa de que se están obteniendo las bases de datos
            print("Aquí tienes el listado de las bases de datos del servidor:")
            # Realiza la consulta para mostrar las bases de datos
            self.cursor.execute("SHOW DATABASES")
            resultado = self.cursor.fetchall()
            # Recorre los resultados y los muestra por pantalla
            for bd in resultado:
                print(f"-{bd[0]}.")
        except:
            # Si ocurre una excepción, se avisa en la consola
            print("No se pudieron obtener las bases de datos. Comprueba la conexión con el servidor.")
        
    #eliminar base de datos  
    @conexion
    @informe_bd      
    def eliminar_bd (self, nombre_bd):
        try:
            self.cursor.execute(f"DROP DATABASE {nombre_bd}")
            print(f"Se elimino la base de datos {nombre_bd}")
        except:
            print(f"La base de datos que intentas eliminar ya se elimino {nombre_bd}")
            print("Estas son las bases de datos disponibles: ")
            #self.mostrar_bd()
            
    #crear bases de datos
    @conexion
    @informe_bd 
    def crear_bd (self, nombre_bd):
        try: 
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {nombre_bd}")
            print(f"Se creo la bases de datos correctamente {nombre_bd}")
            #self.mostrar_bd()
        except:
            print(f"A ocurrido un problema intenta arrgelar el nombre {nombre_bd}")
            print("Estas son las bases de datos disponibles: ")
            #self.mostrar_bd()
     
    @conexion       
    def copia_bd(self, nombre_bd):
        #Verifica si la base de datos existe en el servirdor 
       sql = f"SHOW DATABASES LIKE '{nombre_bd}'"
       self.cursor.execute(sql)
       resultado = self.cursor.fetchone()
        #Si la base de datos no existe, muestra un mensaje de error 
       if not resultado:
            print(f"La base de datos {nombre_bd} no exite")
            return
        #agrega fecha y hora 
       self.fecha_hora = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        #copia de seguridad en la base de datos
       with open(f'{carpeta_respaldo}/{nombre_bd}_{self.fecha_hora}.sql', 'w') as out:
        subprocess.Popen(f'"C:/Program Files/MySQL/MySQL Workbench 8.0/"mysqldump --user=root --password={self.
        contraseña} --databases {nombre_bd}', shell=True, stdout=out)
    
    @conexion    
    def crear_tabla(self, nombre_bd, nombre_tabla, columnas):
      try: 
        #para guardar el string con las columnas y tipos de datos
        columnas_string = ""
        #Se itera la lista que se le pasa como argumento (cada diccionario)
        for columna in columnas:
            #formamos el string con nombre, tipo y longitud
            columnas_string += f"{columna['name']} {columna['type']}({columna['length']})"
            #Si es clave primaria, auto_increment o no adminte valores nulos, lo añade al string
            if columna['primary_key']:
                columnas_string += " PRIMARY KEY"
            if columna['auto_increment']:
                columnas_string += " AUTO_INCREMENT"
            if columna['not_null']:
                columnas_string += " NOT NULL"
            #Hace un salto de línea después de cada diccionario    
            columnas_string += ",\n"
        #Elimina al final del string el salto de línea y la coma    
        columnas_string = columnas_string[:-2]
        #Le indica que base de datos utilizar
        self.cursor.execute(f"USE {nombre_bd}")
        #Se crea la tabla juntando la instrucción SQL con el string generado
        sql = f"CREATE TABLE {nombre_tabla} ({columnas_string});"
        #Se ejecuta la instrucción
        self.cursor.execute(sql)
        #Se hace efectiva
        self.conector.commit()
        #Se cierra la conexión con el servidor
        print("Se cerro la tabla correctamente")
      except:
        print("Ocurrio un error al intentar crear la tabla")      
    
    @conexion
    def eliminar_tabla (self, nombre_bd, nombre_tabla):
      try:
        self.cursor.execute(f"USE {nombre_bd}")
        self.cursor.execute(f"DROP TABLE {nombre_tabla}")
        print(f"La tabl'{nombre_tabla}' eliminada correctamente de la base de datos {nombre_bd}")
      except:
          print(f"No se pudo eliminar la tabla {nombre_tabla} de la bases de datos {nombre_bd}")
          
    @conexion   
    def mostrar_tablas(self, nombre_bd):
        # Primero, se verifica si la base de datos existe
        sql = f"SHOW DATABASES LIKE '{nombre_bd}'"
        self.cursor.execute(sql)
        resultado = self.cursor.fetchone()
        # Si la base de datos no existe, muestra un mensaje de error y termina la función
        if not resultado:
            print(f'La base de datos {nombre_bd} no existe.')
            return
        # Se selecciona la base de datos
        self.cursor.execute(f"USE {nombre_bd};")
        # Se informa de que se están obteniendo las tablas
        print("Aquí tienes el listado de las tablas de la base de datos:")
        # Realiza la consulta para mostrar las tablas de la base de datos actual
        self.cursor.execute("SHOW TABLES")
        resultado = self.cursor.fetchall()
        # Recorre los resultados y los muestra por pantalla
        for tabla in resultado:
            print(f"-{tabla[0]}.")
            
    @conexion
    def mostrar_columnas(self, nombre_bd, nombre_tabla):
        # Primero, se verifica si la base de datos existe
        sql = f"SHOW DATABASES LIKE '{nombre_bd}'"
        self.cursor.execute(sql)
        resultado = self.cursor.fetchone()
        # Si la base de datos no existe, muestra un mensaje de error y termina la función
        if not resultado:
            print(f'La base de datos {nombre_bd} no existe.')
            return
        # Establece la base de datos actual
        self.cursor.execute(f"USE {nombre_bd}")
        try:
            # Realiza la consulta para mostrar las columnas de la tabla especificada
            self.cursor.execute(f"SHOW COLUMNS FROM {nombre_tabla}")
            resultado = self.cursor.fetchall()
            print(resultado)
            # Se informa de que se están obteniendo las columnas
            print(f"Aquí tienes el listado de las columnas de la tabla '{nombre_tabla}':")
            # Recorre los resultados y los muestra por pantalla
            for columna in resultado:
                print(f"-{columna[0]}.")
        except:
            print("Ocurrió un error. Comprueba el nombre de la tabla.")
            
   
          
          
    
   


    
        
    
    

      