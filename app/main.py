from fastapi import FastAPI, HTTPException
import pandas as pd
from pathlib import Path

app = FastAPI()
current_dir = Path.cwd()

hulu = pd.read_parquet(current_dir / 'app/hulu.parquet')
amazon = pd.read_parquet(current_dir / 'app/amazon.parquet')
disney = pd.read_parquet(current_dir / 'app/disney.parquet')
netflix = pd.read_parquet(current_dir / 'app/netflix.parquet')

platforms = {
  "hulu": hulu,
  "amazon": amazon,
  "disney": disney,
  "netflix": netflix
}

@app.get("/")
def read_root():
  pass

#malo
@app.get('/get_max_duration/{anio}/{plataforma}/{dtype}')
async def get_max_duration(anio: int ,
                           plataforma: str ,
                           dtype: str ):

  df_f = platforms[plataforma.lower()]
  df_f = df_f[df_f["type"] == "movie"]
  df_f["duration_int"] = df_f["duration_int"].replace("g", 0)
  df_f["duration_int"] = df_f["duration_int"].astype(int)
  df_f["release_year"] = df_f["release_year"].astype(int)
  df_f["id"] = df_f["id"].astype(str)
  df_f["duration_type"] = df_f["duration_type"].astype(str)
  df_f = df_f.sort_values('duration_int', ascending=False)
  df_f = df_f.loc[df_f['release_year'] == anio]

  df_f = df_f.loc[df_f['duration_type'] == dtype]
  df_f= df_f.iloc[0]["title"]
  return {'pelicula': df_f}


@app.get('/get_score_count/{plataforma}/{scored}/{anio}')
async def get_score_count(plataforma: str, scored: float, anio: int):
  
  df_2 = pd.read_parquet(current_dir / 'app/numeros.parquet')
  df_2= df_2.groupby("movieId")["rating"].mean().reset_index()
  df_2= pd.DataFrame({'id': df_2["movieId"], 'score': df_2["rating"]})

  select_platform = platforms[plataforma.lower()]
  select_platform = select_platform[ select_platform['type'] == 'movie']
  select_platform= select_platform.loc[select_platform['release_year'] == anio]

  df_2= pd.merge(select_platform, df_2, on="id")
  df_2= df_2[df_2['score'] > scored]

  return {
        'plataforma': plataforma,
        'cantidad': len(df_2),
        'anio': anio,
        'score': scored 
    }


@app.get('/get_count_platform/{plataforma}')
async def get_count_platform(plataforma: str):
  select_platform = platforms[plataforma.lower()]
  return {'plataforma': plataforma, 'peliculas': len(select_platform)}


@app.get('/get_actor/{plataforma}/{anio}')
async def get_actor(plataforma: str, anio: int):
  platform_arg = plataforma.lower()
  if platform_arg in platforms.keys():
    plataf = platforms[platform_arg]

  else:
    raise HTTPException(status_code=400, detail="La plataform no se encuentra")

  plataf = plataf[ plataf["release_year"] == anio]
  plataf = plataf[ plataf["cast"] != 'g']
  plataf = plataf[ plataf["cast"] != '1']
  nombres_actores = plataf['cast'].str.split(',|:').explode().str.strip()
  conteo_actores = nombres_actores.value_counts()
  actor = str(conteo_actores.index[0])
  apariciones = int(conteo_actores[0])

  return {
        'plataforma': plataforma,
        'anio': anio,
        'actor': actor,
        'apariciones': apariciones 
    }


@app.get('/prod_per_county/{tipo}/{pais}/{anio}')
async def prod_per_county(tipo: str, pais: str, anio: int):
  df_f = pd.concat([hulu, amazon, disney, netflix])
  pais_l = pais.lower()
  tipo_enum = ['tv show', 'movie']

  df_f = df_f[ df_f['release_year'] == anio]
  df_f = df_f[ df_f['country'] == pais_l]
  if tipo in tipo_enum:
    df_f = df_f[df_f["type"] == tipo]
  else: 
    raise HTTPException(status_code=400, detail="El tipo no se encuentra")

  respuesta = df_f.shape[0]
  print(respuesta)
  
  return {'pais': pais, 'anio': anio, 'peliculas': respuesta}


@app.get('/get_contents/{rating}')
async def get_contents(rating: str):
  df_f = pd.concat([hulu, amazon, disney, netflix])
  df_f = df_f[ df_f["rating"] == rating]
  respuesta = df_f.shape[0]

  return {'rating': rating, 'contenido': respuesta}


# @app.get('/get_recomendation/{title}')
# def get_recomendation(title,):
#     return {'recomendacion':respuesta}