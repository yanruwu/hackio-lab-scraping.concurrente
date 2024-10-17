import concurrent.futures
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from time import sleep
from src.dict_municipios import dict_municipios

def tables2(tabletext, tablehtml):
    """
    Convierte el texto y el HTML de una tabla en un DataFrame de pandas.

    Esta función toma el texto de una tabla meteorológica y su correspondiente HTML,
    extrae los encabezados y organiza los datos en un DataFrame con columnas específicas.

    Args:
        tabletext (str): Texto de la tabla que contiene datos meteorológicos.
        tablehtml (str): HTML de la tabla que se utiliza para extraer encabezados.

    Returns:
        pd.DataFrame: DataFrame que contiene los datos organizados con los encabezados nuevos.
    """
    soup2 = BeautifulSoup(tablehtml, "html.parser")
    headers2 = [h.text for h in soup2.find("thead").findAll("th")]
    amounts = ["High", "Avg", "Low"]
    new_headers = ["Date"] + [f"{a} {h}" for h in headers2[1:5] for a in amounts] + [f"{a} {headers2[5]}" for a in ["High", "Low"]] + [headers2[-1]]

    table2 = tabletext.replace("°F ", "").replace("% ", "").replace("mph ", "").replace("in", "").split()
    df_table2 = pd.DataFrame(np.reshape(table2, (len(table2) // 16, 16)), columns=new_headers)
    
    return df_table2

def obtener_datos_municipio(municipio, code, max_retries=3):
    """
    Obtiene datos meteorológicos de un municipio específico desde un sitio web.

    Esta función utiliza Selenium para acceder a los datos de un municipio a partir
    de un código proporcionado. Intenta realizar la operación un número máximo de veces
    en caso de que ocurra un error, y compila los datos mensuales en un DataFrame.

    Args:
        municipio (str): Nombre del municipio para el cual se obtienen los datos.
        code (str): Código del municipio que se utiliza para construir la URL de acceso.
        max_retries (int, optional): Número máximo de reintentos en caso de fallo de conexión. Por defecto es 3.

    Returns:
        pd.DataFrame: DataFrame que contiene los datos meteorológicos del municipio, o un DataFrame vacío si no se pudieron obtener datos.
    """
    df_municipio = pd.DataFrame()
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    retries = 0
    while retries < max_retries:
        try:
            for monthid in range(10, -1, -1):
                if monthid == 0:
                    break
                
                url_getres = f"https://www.wunderground.com/dashboard/pws/{code}/table/2024-{monthid}-16/2024-{monthid}-16/monthly"
                driver.get(url_getres)
                driver.execute_script("window.scrollTo(0, 1600)")

                WebDriverWait(driver, 10).until(EC.presence_of_element_located(("xpath", '//*[@id="main-page-content"]/div/div/div/lib-history/div[2]/lib-history-table/div/div/div/table/tbody')))

                table = driver.find_element("xpath", '//*[@id="main-page-content"]/div/div/div/lib-history/div[2]/lib-history-table/div/div/div/table/tbody').text
                table_html2 = driver.find_element("xpath", '//*[@id="main-page-content"]/div/div/div/lib-history/div[2]/lib-history-table/div/div/div/table').get_attribute("innerHTML")

                df_month = tables2(table, table_html2)
                df_municipio = pd.concat([df_municipio, df_month])

            df_municipio.insert(loc=0, column="Municipio", value=municipio)
            
            
            return df_municipio

        except Exception as e:
            print(f"Error al procesar {municipio} ({code}): {e}")
            retries += 1
            sleep(2) 
            if retries == max_retries:
                print(f"Máximo de reintentos alcanzado para {municipio}. Se omitirá.")
                break 

        finally:
            driver.quit()

    return df_municipio 