import pyreadstat
import pandas as pd

# Cargar el archivo .sav
input_sav_path = "files/Vinson Datos 2019_2024-R2-N.sav"
df, meta = pyreadstat.read_sav(input_sav_path)

# Guardar como .csv
output_csv_path = "files/Vinson_Datos_2019_2024-N.csv"
df.to_csv(output_csv_path, index=False)

print(f"Archivo convertido y guardado en {output_csv_path}")
