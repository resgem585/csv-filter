import os
import pandas as pd
from data_process import load_and_process_data
from tqdm import tqdm


def main():
    # Definir la ruta del archivo de entrada
    input_file_path = "files/Vinson_Datos_2019_2024-R1-N.csv"

    # Cargar los datos en partes con tqdm
    print("Cargando datos...")
    data_iter = pd.read_csv(
        input_file_path, low_memory=False, iterator=True, chunksize=1000
    )
    data = pd.concat(
        [chunk for chunk in tqdm(data_iter, desc="Cargando datos")], ignore_index=True
    )

    # Procesar los datos con tqdm
    print("Procesando datos...")
    data = load_and_process_data(data)  # Aquí se pasa el DataFrame "data" directamente

    # Crear la carpeta de salida si no existe
    print("Creando directorio de salida...")
    with tqdm(total=1, desc="Creando carpeta de salida") as pbar:
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        pbar.update(1)

    # Guardar los datos procesados con barra de progreso
    print("Guardando datos...")
    with tqdm(total=len(data), desc="Guardando datos") as pbar:
        output_file_path = os.path.join(output_dir, "processed_data_1.csv")
        data.to_csv(output_file_path, index=False, encoding="utf-8-sig")
        pbar.update(len(data))

    print(f"Archivo guardado en: {output_file_path}")


if __name__ == "__main__":
    main()
