from fastapi import FastAPI
import pandas as pd
# import requests
# from io import StringIO
from pathlib import Path

app = FastAPI()
current_dir = Path.cwd()

# def get_url(url):
#     file_id = url.split('/')[-2]
#     dwn_url = 'https://drive.google.com/uc?export=download&id=' + file_id
#     url2 = requests.get(dwn_url).text
#     csv_raw = StringIO(url2)
#     return csv_raw
# df = pd.read_parquet(csv_raw)
# print(df.head())
# file_id = url.split('/')[-2]
# dwn_url = 'https://drive.google.com/uc?id=' + file_id
# return dwn_url

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

# df_2 = pd.read_parquet(get_url(
#     "https://drive.google.com/file/d/1mnibRptgg4cs8X0AyuNm7u8pZenMCrK8/view?usp=sharing"))

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


# todo : el duration_type es irrelevante
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


# Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año
# -> int
@app.get("/get_score_count/")
async def get_score_count(platform: str, scored: float, year: int):
  # df_2 = pd.concat([uno , dos , tres, cuatro , cinco , seis, siete, ocho])
  # print(df_2)
  score = df_2.groupby("movieId")["rating"].mean().reset_index()
  new_df = pd.DataFrame({'id': score["movieId"], 'score': score["rating"]})

  select_platform = platforms[platform.lower()]
  platform_df = select_platform.loc[select_platform['release_year'] == year]
  merged_df = pd.merge(platform_df, new_df, on="id")
  merged_df = merged_df[merged_df['score'] > scored]
  # return len(merged_df)
  return {
        'plataforma': platform,
        'cantidad': len(merged_df),
        'anio': year,
        'score': scored 
    }



# Cantidad de películas por plataforma con filtro de PLATAFORMA.
# -> int
@app.get("/get_count_platform/")
async def get_count_platform(platform: str):
  select_platform = platforms[platform.lower()]
  # return len(select_platform)
  return {'plataforma': platform, 'peliculas': len(select_platform)}


# Actor que más se repite según plataforma y año.
# -> string
# todo: reparar
@app.get("/get_actor/")
async def get_actor(platform: str, year: int):
  platform_arg = platform.lower()
  if platform_arg in platforms.keys():
    df_f = df.loc[df['id'].str[0] == platform_arg[0]]
  else:
    df_f = df

  df_f = df_f.loc[df_f['release_year'] == year]

  nombres_actores = df_f['cast'].str.split(',|:').explode()
  nombres_actores = nombres_actores.str.strip()
  conteo_actores = nombres_actores.value_counts()

  df_actores = pd.DataFrame({
    'actor': conteo_actores.index,
    'conteo': conteo_actores.values
  })

  df_actores = df_actores.drop(0).reset_index(drop=True)

  # return df_actores["actor"][0]
  return {
        'plataforma': plataform,
        'anio': year,
        'actor': respuesta,
        'apariciones': respuesta
    }

  # return {'rating': rating, 'contenido': respuesta}


# tv show  ( streaming)
# cantidad de contenidos que se publico por pais / ano
# -> {'pais': pais, 'anio': anio, 'peliculas': respuesta :int}
@app.get("/prod_per_county/")
async def prod_per_county(type: str, country: int, year: int):
  pais = "pais"
  anio = "anio"
  respuesta = 4

  return {'pais': pais, 'anio': anio, 'peliculas': respuesta}


# streaming, peliculas
# -> int


@app.get("/get_contents/")
async def get_contents(rating: int):
  respuesta = 4
  return {'recomendacion': respuesta}
