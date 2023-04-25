# ProjectVGM

Este proycto contiene todo el codigo preparado para el desarrollo del proyecto nombrado ProjectVGM. Este proyecto tiene como objetivo establecer un sistema de ETL de datos relacionados con activos financieros así como el estudio de distintas estratégias para establecer un modelo de anticipación de las variaciones de los activos de cara a un potencial algoritmo de inversión autónomo o un posible recomendador personal.

Este proyecto sirve como un repositorio único de distintos archivos, proyectos y pruebas varias. Con esto, se implica que no existe una estructura única y coherente entre todo el mismo proyecto sinó que cada archivo realiza una cierta tarea de manera indistinta. A continuación lo que se expone consiste en un listado de los distintos módulos presentados y una breve descripción de su propósito y contenido.

## Utils
Este módulo consiste en un package de utilidades varias usadas en el proyecto. Concretamente se incluye el módulo `CustomPlotter` que recoge un conjunto genérico de funcionalidades de graficación de los datos y la clase `DateCursor` que proporciona una serie de funcionalidades usadas en el proceso de descarga de datos para facilitar el proceso de descarga de datos con consultas a APIs paginadas según fecha.

## Alpaca API
Este módulo contiene las funcionalidades de descarga de datos de la API de Alpaca. Este módulo presenta funcionalidades para descargar cualquier tipo de dato de la API para guardar-los en un CSV.

Addicionalmente, se ha encapsulado en este módulo el modulo _AlphaVantage.py_ que realiza la misma funcionalidad pero para la API de AlphaVantage.

En resumen, la API de Alpaca sirve para realizar la descarga de los datos del historico de precios. Y, por otro lado, AlphaVantage se ha usado para la descarga de los datos financieros de las empresas así como la obtención de datos històricos de indicadores macroeconómicos.

## Reddit
Este paquete tiene como objetivo la recolección de comentarios y _posts_ de la red social Reddit en relación a los activos económicos con los que se ha tratado.

## ETL_Notebooks
En este directorio se recoge los Jupyter Notebooks usados con la finalidad de procesar los datos de los módulos anteriores para obtener un único dataset final con todos los datos listos para su procesado.

En este directorio se dispone de:
- `barsETL.ipynb`: Dedicado al preprocesado de los datos del historico de precios de los distintos activos para extraer todos los indicadores derivados de este.
- `financialetl.ipynb`: Este notebook se dedica a preprocesar todos los datos financieros de los que se dispone así como su integración a los datos històricos de los precios de los activos.

## EDA_Notebooks
En este directorio se encuentran los notebooks, tanto los usados para tareas específicas de EDA como los usados para preparar los datos, transformarlos, entrenar modelos y evaluarlos.

Los notebooks más relevantes a tener en cuenta són:
- `test_eda.ipynb`: Este fichero se enfoca a la parte de EDA así como la predicción de variaciones en modo regresión
- `prediction.ipynb`: Este notebook se enfoca en la tarea de transformación y obtención de las categorías de las instáncias para enfocar el proyecto como una clasificación.
