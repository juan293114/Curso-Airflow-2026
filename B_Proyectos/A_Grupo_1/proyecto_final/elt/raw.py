import requests
import json
from pathlib import Path


def ingest_raw(raw_path, timeout=15, **kwargs):
    """
    Descarga productos de FakeStore y guarda un JSON en raw_path.
    Inputs: raw_path (str o Path) archivo de salida, timeout (int).
    Output: str con la ruta del archivo guardado.
    """
    api_url = "https://fakestoreapi.com/products"
    
    raw_path = Path(raw_path)
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        print(f"Conectando a la fuente: {api_url}")
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()

        # Persistencia
        with open(raw_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"Éxito: Datos almacenados en {raw_path}")
        print(f"Registros capturados: {len(data)}")
        
        return str(raw_path)

    except Exception as e:
        print(f"Fallo en la ingesta: {str(e)}")
        raise
