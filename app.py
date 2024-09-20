import bd.bases_datos as sqlbd
import bd.tablas as tbl

base_datos = sqlbd.BasesDatos(**sqlbd.conexion)

base_datos.eliminar_bd("Edison")


