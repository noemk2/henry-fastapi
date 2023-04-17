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

uno = pd.read_parquet(current_dir / 'app/ratings/1.parquet')
dos = pd.read_parquet(current_dir / 'app/ratings/2.parquet')
tres = pd.read_parquet(current_dir / 'app/ratings/3.parquet')
cuatro = pd.read_parquet(current_dir / 'app/ratings/4.parquet')
cinco = pd.read_parquet(current_dir / 'app/ratings/5.parquet')
seis = pd.read_parquet(current_dir / 'app/ratings/6.parquet')
siete = pd.read_parquet(current_dir / 'app/ratings/7.parquet')
ocho = pd.read_parquet(current_dir / 'app/ratings/8.parquet')

df_2 = pd.concat([uno, dos, tres, cuatro, cinco, seis, siete, ocho])
df = pd.concat([hulu, amazon, disney, netflix])


@app.get("/")
def read_root():
  pass
  # return {hulu.keys()[0]}

@app.get("/get_max_duration/")
async def get_max_duration(year: int = None,
                           platform: str = None,
                           duration_type: str = None):
  df_f = df[df["type"] == "movie"]
  df_f["duration_int"] = df_f["duration_int"].replace("g", 0)

  df_f["duration_int"] = df_f["duration_int"].astype(int)
  df_f["release_year"] = df_f["release_year"].astype(int)
  df_f["id"] = df_f["id"].astype(str)
  df_f["duration_type"] = df_f["duration_type"].astype(str)

  df_f = df_f.sort_values('duration_int', ascending=False)

  if year is not None:
    df_f = df_f.loc[df_f['release_year'] == year]
  if platform is not None:
    platform_arg = platform.lower()
    if platform_arg in platforms.keys():
      df_f = df_f.loc[df_f['id'].str[0] == platform_arg[0]]
    else:
      pass

  if duration_type is not None:
    df_f = df_f.loc[df_f['duration_type'] == duration_type]
  else:
    respuesta = df_f.iloc[0]["title"]
    return {'pelicula': respuesta}

  respuesta = df_f.iloc[0]["title"]
  return {'pelicula': respuesta}


@app.get("/get_score_count/")
async def get_score_count(plataforma: str, scored: float, anio: int):
  # df_2 = pd.concat([uno , dos , tres, cuatro , cinco , seis, siete, ocho])
  # print(df_2)
  
  score = df_2.groupby("movieId")["rating"].mean().reset_index()
  new_df = pd.DataFrame({'id': score["movieId"], 'score': score["rating"]})

  select_platform = platforms[plataforma.lower()]
  select_platform = select_platform[ select_platform['type'] == 'movie']

  platform_df = select_platform.loc[select_platform['release_year'] == anio]

  merged_df = pd.merge(platform_df, new_df, on="id")
  merged_df = merged_df[merged_df['score'] > scored]
  # return len(merged_df)
  return {
        'plataforma': plataforma,
        'cantidad': len(merged_df),
        'anio': anio,
        'score': scored 
    }


@app.get("/get_count_platform/")
async def get_count_platform(platform: str):
  select_platform = platforms[platform.lower()]
  # return len(select_platform)
  return {'plataforma': platform, 'peliculas': len(select_platform)}


@app.get("/get_actor/")
async def get_actor(platform: str, year: int):
  platform_arg = platform.lower()
  print("hola hola                    HOLA")
  if platform_arg in platforms.keys():
    plataf = platforms[platform_arg]

  else:
    raise HTTPException(status_code=400, detail="La plataform no se encuentra")

  plataf = plataf[ plataf["release_year"] == year]
  plataf = plataf[ plataf["cast"] != 'g']
  plataf = plataf[ plataf["cast"] != '1']
  nombres_actores = plataf['cast'].str.split(',|:').explode().str.strip()
  conteo_actores = nombres_actores.value_counts()
  actor = str(conteo_actores.index[0])
  apariciones = int(conteo_actores[0])

  return {
        'plataforma': platform,
        'anio': year,
        'actor': actor,
        'apariciones': apariciones 
    }


@app.get("/prod_per_county/")
async def prod_per_county(tipo: str, pais: str, anio: int):
  pais_l = pais.lower()
  tipo_enum = ['tv show', 'movie']

  df_f = df[ df['release_year'] == anio]
  df_f = df_f[ df_f['country'] == pais_l]
  if tipo in tipo_enum:
    df_f = df_f[df_f["type"] == tipo]
  else: 
    raise HTTPException(status_code=400, detail="El tipo no se encuentra")

  respuesta = df_f.shape[0]
  print(respuesta)
  
  return {'pais': pais, 'anio': anio, 'peliculas': respuesta}


@app.get("/get_contents/")
async def get_contents(rating: str):
  respuesta = 4
  return {'recomendacion': respuesta}
