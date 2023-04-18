from fastapi import FastAPI
import pandas as pd
from pathlib import Path

app = FastAPI()
current_dir = Path.cwd()

platforms = {
  "hulu": current_dir / 'app/hulu.parquet',
  "amazon": current_dir / 'app/hulu.parquet',
  "disney": current_dir / 'app/hulu.parquet',
  "netflix": current_dir / 'app/hulu.parquet',
}


def get_rating():
  df = pd.concat([
  pd.read_parquet(platforms["hulu"]),
  pd.read_parquet(platforms["amazon"]),
  pd.read_parquet(platforms["disney"]),
  pd.read_parquet(platforms["netflix"]),
    ]) 
  return df

# def get_df_2():
#   df_2 = pd.read_parquet(current_dir / "app/numeros_f.parquet")
#   df_2= df_2.groupby("movieId")["rating"].mean().reset_index()
#   return df_2

def get_select_plataform(plataforma, anio):
  select_platform = pd.read_parquet(platforms[plataforma.lower()])
  select_platform = select_platform.query('type == "movie" and release_year == @anio')
  return select_platform

def get_merge(df_1, df_2, scored) -> int:
  df_f = pd.merge(df_1, df_2, left_on='id', right_on="movieId")
  df_f = df_f.query('rating_y > @scored')
  return df_f.shape[0]


@app.get("/")
def read_root():
  pass

@app.get('/get_max_duration/{anio}/{plataforma}/{dtype}')
async def get_max_duration(anio: int ,
                           plataforma: str ,
                           dtype: str ):

  df_f = pd.read_parquet(platforms[plataforma.lower()])
  df_f = df_f[df_f["type"] == "movie"]
  df_f["duration_int"] = df_f["duration_int"].replace("g", 0)
  df_f["duration_int"] = df_f["duration_int"].astype(int)
  df_f = df_f.sort_values('duration_int', ascending=False)
  df_f = df_f.query('release_year == @anio and duration_type == @dtype')
  df_f = df_f.iloc[0]["title"]
  return {'pelicula': df_f}

@app.get('/get_score_count/{plataforma}/{scored}/{anio}')
async def get_score_count(plataforma: str, scored: float, anio: int):
  # df_2 = get_df_2()
  df_2 = pd.read_parquet(current_dir /"app/numeros_ff.parquet")

  select_platform = get_select_plataform(plataforma, anio)
  resultado = get_merge(select_platform, df_2,  scored)

  return {
        'plataforma': plataforma,
        'cantidad': resultado,
        'anio': anio,
        'score': scored 
    }

@app.get('/get_count_platform/{plataforma}')
async def get_count_platform(plataforma: str):
  select_platform = platforms[plataforma.lower()]
  return {'plataforma': plataforma, 'peliculas': len(select_platform)}

@app.get('/get_actor/{plataforma}/{anio}')
async def get_actor(plataforma: str, anio: int):
  plataf = pd.read_parquet(platforms[plataforma.lower()])
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
  df = get_rating()
  df_f = df.loc[(df['release_year'] == anio) & (df['country'] == pais.lower()) & (df['type'] == tipo)]
  respuesta = df_f.shape[0]
  return {'pais': pais, 'anio': anio, 'peliculas': respuesta}

@app.get('/get_contents/{rating}')
async def get_contents(rating: str):
  df = get_rating()
  df_f = df.loc[df["rating"] == rating]
  respuesta = df_f.shape[0]
  return {'rating': rating, 'contenido': respuesta}
 
# @app.get('/get_recomendation/{title}')
# def get_recomendation(title,):
    
#     return {'recomendacion':respuesta}