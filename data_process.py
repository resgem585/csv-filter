import pandas as pd
from tqdm import tqdm


def load_and_process_data(data):
    # Convertir la columna 'edadx' a enteros
    data["edadx"] = data["edadx"].astype(int)

    # Mapeo de los valores numéricos de las columnas a sus equivalentes de texto
    sexo_map = {1: "Masculino", 2: "Femenino"}
    data["sexo"] = data["sexo"].map(sexo_map)

    nse_map = {1: "AB", 2: "C+/C", 3: "C-/D+/D"}
    data["nse_nue"] = data["nse_nue"].map(nse_map)

    ciudad_map = {
        1: "Distrito Federal",
        2: "Guadalajara",
        3: "Monterrey",
        4: "Merida",
        5: "Puebla",
        6: "Hermosillo",
        7: "Morelia",
        8: "Área Conurbada",
    }
    data["ciudad"] = data["ciudad"].map(ciudad_map)

    ecat_map = {
        1: "12-15",
        2: "16-19",
        3: "20-24",
        4: "25-29",
        5: "30-34",
        6: "35-39",
        7: "40-44",
        8: "45-49",
        9: "50-54",
        10: "55-65",
    }
    data["ecat"] = data["ecat"].map(ecat_map)

    medicion_map = {
        26: "Q1 2019",
        27: "Q2 2019",
        28: "Q3 2019",
        29: "Q4 2019",
        30: "Q1 2020",
        31: "Q2 2020",
        32: "Q3 2020",
        33: "Q4 2020",
        34: "Q1 2021",
        35: "Q2 2021",
        36: "Q3 2021",
        37: "Q4 2021",
        38: "Q1 2022",
        39: "Q2 2022",
        40: "Q3 2022",
        41: "Q4 2022",
        42: "Q1 2023",
        43: "Q2 2023",
        44: "Q3 2023",
        45: "Q4 2023",
        46: "Q1 2024",
        47: "Q2 2024",
    }
    data["medicion_Q"] = data["medicion_Q"].map(medicion_map)

    anio_map = {19: "2019", 20: "2020", 21: "2021", 22: "2022", 23: "2023", 24: "2024"}
    data["anio"] = data["anio"].map(anio_map)

    panel_map = {1: "NetQuest", 2: "Offerwise"}
    data["panel"] = data["panel"].map(panel_map)

    enlinea_map = {1: "En línea", 2: "Cara a cara"}
    data["enlinea"] = data["enlinea"].map(enlinea_map)

    b2f_r12_map = {1: "Sí", 2: "No"}
    data["b2f.r12"] = data["b2f.r12"].map(b2f_r12_map)

    leales_map = switcher_map = potencial_map = esporadico_map = no_interesados_map = (
        abandonador_map
    ) = {1: "Sí"}
    data["leales"] = data["leales"].map(leales_map)
    data["switcher"] = data["switcher"].map(switcher_map)
    data["potencial"] = data["potencial"].map(potencial_map)
    data["esporadico"] = data["esporadico"].map(esporadico_map)
    data["no_interesados"] = data["no_interesados"].map(no_interesados_map)
    data["abandonador"] = data["abandonador"].map(abandonador_map)

    marcas_map = {
        "Bonafont": 1,
        "Ciel": 2,
        "E.Pura": 3,
        "Pureza Vital": 4,
        "Santa María": 5,
        "Skarch": 6,
    }

    # Reemplazar los nombres de marcas en las filas con sus valores numéricos
    for marca, valor in marcas_map.items():
        data.replace({marca: valor}, inplace=True)

    # Crear una lista para almacenar las filas transformadas
    transformed_rows = []

    atributos = [
        "b8c Siempre tiene comerciales",
        "b8c Tiene la mejor publicidad",
        "b8c Tiene la botella más atractiva",
        "b8c Es para todos los días",
        "b8c Es una marca que amo",
        "b8c Da la seguridad de que está 100% purificada",
        "b8c Está más a la vista cuando la voy a comprar",
        "b8c Es ligera",
        "b8c Me ayuda a tomar más agua",
        "b8c Me ayuda a mejorar mi salud",
        "b8c Tiene buen sabor",
        "b8c Tiene un empaque práctico",
        "b8c Es amigable con el medio ambiente",
        "b8c Tiene todo lo que debe de tener el agua",
        "b8c Me ayuda a que todo mi cuerpo se active",
        "b8c Me ayuda a eliminar todo lo malo",
        "b8c Me ayuda a que todo mi cuerpo funcione correctamente",
        "b8c Es para cualquier actividad",
        "b8c Está presente en eventos atractivos para mi",
        "b8c Tiene promociones atractivas",
        "b8c Es innovadora",
        "b8c Ofrece algo diferente a las otras marcas",
        "b8c Es refrescante",
        "b8c Me mantiene bien hidratado",
        "b8c Me hace consciente de lo maravilloso que es mi cuerpo",
        "b8c Me ayuda a cuidar mi cuerpo",
        "b8c Me ayuda a estar en armonía",
        "b8c Es la que me da más valor por mi dinero",
        "b8c Se comporta responsablemente con el medio ambiente",
    ]

    # Progreso para procesar datos
    for index, row in tqdm(data.iterrows(), total=len(data), desc="Procesando datos"):
        for i, atributo in enumerate(atributos):
            new_row = {marca: "" for marca in marcas_map.keys()}
            new_row["Atributos"] = atributo

            for marca in marcas_map.keys():
                column_name = f"img_{30 + i}_{marcas_map[marca]}"
                if column_name in data.columns and pd.notna(row[column_name]):
                    new_row[marca] = int(marcas_map[marca])

            new_row["folio"] = row["folio"]
            new_row["nse_nue"] = row["nse_nue"]
            new_row["ciudad"] = row["ciudad"]
            new_row["ecat"] = row["ecat"]
            new_row["edadx"] = row["edadx"]
            new_row["sexo"] = row["sexo"]
            new_row["medicion_Q"] = row["medicion_Q"]
            new_row["anio"] = row["anio"]
            new_row["panel"] = row["panel"]
            new_row["enlinea"] = row["enlinea"]
            new_row["b2f.r12"] = row["b2f.r12"]
            new_row["leales"] = row["leales"]
            new_row["switcher"] = row["switcher"]
            new_row["potencial"] = row["potencial"]
            new_row["esporadico"] = row["esporadico"]
            new_row["no_interesados"] = row["no_interesados"]
            new_row["abandonador"] = row["abandonador"]

            new_row["b1a.r05"] = row["b1a.r05"]
            new_row["b1a.r06"] = row["b1a.r06"]
            new_row["b1a.r12"] = row["b1a.r12"]
            new_row["b1a.r19"] = row["b1a.r19"]
            new_row["b1a.r21"] = row["b1a.r21"]
            new_row["b1a.r28"] = row["b1a.r28"]
            new_row["b1b.r05"] = row["b1b.r05"]
            new_row["b1b.r06"] = row["b1b.r06"]
            new_row["b1b.r12"] = row["b1b.r12"]
            new_row["b1b.r19"] = row["b1b.r19"]
            new_row["b1b.r21"] = row["b1b.r21"]
            new_row["b1b.r28"] = row["b1b.r28"]
            new_row["b2a.r05"] = row["b2a.r05"]
            new_row["b2a.r06"] = row["b2a.r06"]
            new_row["b2a.r12"] = row["b2a.r12"]
            new_row["b2a.r19"] = row["b2a.r19"]
            new_row["b2a.r21"] = row["b2a.r21"]
            new_row["b2a.r28"] = row["b2a.r28"]

            # Asignar ponderación (ajústalo según cómo se calcule en tus datos)
            new_row["ponderacion"] = row.get(
                "ponderacion", 1.63
            )  # Ajuste por defecto a 1.63

            # Agregar la fila nueva a la lista
            transformed_rows.append(new_row)

    # Convertir la lista de filas transformadas en un DataFrame
    transformed_df = pd.DataFrame(transformed_rows)

    # Reordenar las columnas según el orden especificado
    columns_order = [
        "folio",
        "nse_nue",
        "ciudad",
        "ecat",
        "edadx",
        "sexo",
        "medicion_Q",
        "anio",
        "panel",
        "enlinea",
        "b1a.r05",
        "b1a.r06",
        "b1a.r12",
        "b1a.r19",
        "b1a.r21",
        "b1a.r28",
        "b1b.r05",
        "b1b.r06",
        "b1b.r12",
        "b1b.r19",
        "b1b.r21",
        "b1b.r28",
        "b2a.r05",
        "b2a.r06",
        "b2a.r12",
        "b2a.r19",
        "b2a.r21",
        "b2a.r28",
        "Bonafont",
        "Ciel",
        "E.Pura",
        "Pureza Vital",
        "Santa María",
        "Skarch",
        "Atributos",
        "b2f.r12",
        "leales",
        "switcher",
        "potencial",
        "esporadico",
        "no_interesados",
        "abandonador",
        "ponderacion",
    ]

    # Asegurar que todas las columnas existan en el DataFrame final
    for col in columns_order:
        if col not in transformed_df.columns:
            transformed_df[col] = ""

    # Reordenar las columnas
    transformed_df = transformed_df[columns_order]

    return transformed_df
