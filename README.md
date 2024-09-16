# Utilities for accessing historical aerial images from NLS Finland 

Collection of scripts for accessing historical aerial images from [Ortokuvien ja korkeusmallien kyselypalvelu (WCS)](https://www.maanmittauslaitos.fi/ortokuvien-ja-korkeusmallien-kyselypalvelu) by the NLS Finland. 

## Requirements

### Installation

Required packages are `rasterio`, `geopandas` (optionally with `pyogrio`), `owslib`, and `fastcore`. Environment with them can be installed with 

> conda env create -f env.yml

### API-key

In order to access the WCS API, you need to create your own API-key [according to these instructions](https://www.maanmittauslaitos.fi/rajapinnat/api-avaimen-ohje). Afterwards, create the following file in `secret/` named `creds.json`:

```json
{"mml_api_key": "<your_api_key_here>"}

```

so the scripts know where to look for it.

## Usage

The main script `download_mml_data.py` works like the following (output of `python download_mml_data.py -h`):

```bash
usage: download_mml_data.py [-h] [--year_layer_path YEAR_LAYER_PATH] [--id_column ID_COLUMN] [--false_color]
                            [--imsize IMSIZE]
                            locations outpath

Download all available aerial images from `locations` and save them to `outpath`. If `locations` are Point data, all points are buffered so that `imsize` times `imsize` images centered around the points are produced. If `locations` are Polygon data, resulting images cover the orthogonal bounding boxes of the locations. If `year_layer_path` is provided, possible years are filtered so that only years where there is a possibility of aerial campaing are processed. Otherwise, all years from 1931 to 2024 are processed.

positional arguments:
  locations                          Path to GIS data containing locations either as points or polygons
  outpath                            Directory to save the results

options:
  -h, --help                         show this help message and exit
  --year_layer_path YEAR_LAYER_PATH  Data of historical aerial campaigns used to filter possible years to process
  --id_column ID_COLUMN              Which column is used to identify the locations. If None, index is used.
  --false_color                      Whether to download NIR-R-G images. Available only for 2009 and later (default:  False)
  --imsize IMSIZE                    If locations are point data, the size of the images to download (default: 256)
```

The data are saved with the following folder structure

```
    outpath|
        |-location1|
                   |-year1
                   |-year2
                   |-...
        |-location2|
                   |-year1
                   |-year2
                   |-...
                   
```

`download_mml_data.py` downloads multiple files at once with `joblib.Parallel` using as many processes as possible. If you need to limit the number of simultaneous downloads, change the value of `Parallel(n_jobs=-1)` to whichever you need.

### Time layers

As data from each year covers whole Finland so that if there are no aerial images from an area, the data is simply just zero. The default way to download the data is to brute-force all years between 1931 and 2024 and check whether the minimum and maximum values are identical. This is slow especially for large areas or multiple points.

In order to speed up the search, it is possible to use [Ilmakuvausten ja laserkeilausten indeksikartta](https://hkp.maanmittauslaitos.fi/hkp/published/fi/4343c1b4-7d8f-4473-896a-70f930f36be1), which shows all aerial campaigns in Finland since 1931. These layers are available as merged data in `data/index_layers.geojson`, and they contain the merged `ilmakuvat` and `ortot` for each year. The file `data/index_layers.json` is the mapping between layer identifiers and explanation on the index map.

The scripts used to produce these data are `get_time_layers.py` and `flatten_time_layers.py`.