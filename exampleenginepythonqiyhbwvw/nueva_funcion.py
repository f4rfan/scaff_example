from typing import Dict
from py4j.java_gateway import JavaObject
from py4j.protocol import Py4JJavaError


def nuevo_metodo_desarrollado(runtimeContext: JavaObject, root_key: str) -> Dict:
    """
    Returns parameters from pyspark context

    Args:
        runtimeContext: The pyspark context parameters
        root_key: The key where params are setted

    Returns:
        The pyspark context parameters
    """
    config = runtimeContext.getConfig()

    if config.isEmpty():
        print("config vacio")
        return {}

    try:
        return {
            param: {
                param2: config.getString(f"{root_key}.{param}.{param2}")
                for param2 in config.getObject(f"{root_key}.{param}")
                if str(config.getString(f"{root_key}.{param}.{param2}")) != ""
            }
            for param in config.getObject(f"{root_key}")
            if str(config.getObject(f"{root_key}.{param}")) != ""
        }
    except Py4JJavaError:
        return {}
