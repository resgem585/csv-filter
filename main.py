import os
import pandas as pd
from data_process import load_and_process_data
from tqdm import tqdm


def main():
    # Definir la ruta del archivo de entrada
    input_file_path = "files/Vinson_Datos_2019_2024-R1-N.csv"

    # Cargar los datos en partes con tqdm

    data_iter = pd.read_csv(
        input_file_path, low_memory=False, iterator=True, chunksize=1000
    )
    data = pd.concat(
        [chunk for chunk in tqdm(data_iter, desc="Cargando datos")], ignore_index=True
    )

    # Crear una barra de progreso para procesar los datos

    with tqdm(total=len(data), desc="Procesando datos") as pbar:
        data = load_and_process_data(data)  # Pasamos el DataFrame "data" directamente
        pbar.update(len(data))

    # Crear la carpeta de salida si no existe

    with tqdm(total=1, desc="Creando carpeta de salida") as pbar:
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        pbar.update(1)

    # Guardar los datos procesados con barra de progreso

    with tqdm(total=len(data), desc="Guardando datos") as pbar:
        output_file_path = os.path.join(output_dir, "processed_data_1.csv")
        data.to_csv(output_file_path, index=False, encoding="utf-8-sig")
        pbar.update(len(data))

    print(f"Archivo guardado en: {output_file_path}")


if __name__ == "__main__":
    main()
