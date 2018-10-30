"""
This module is temporary. It should be updated to use
Censo microdata.
"""
import logging
import yaml
import json
from enum import IntEnum

import unissono.datasets
from unissono.util import strip_diacritics

logger = logging.getLogger(__name__)

_API_SHAPE_TEMPLATE = "http://www.censo2010.ibge.gov.br/apps/areaponderacao/shapes/AP_C10_%d_10000.json"
_API_VARIABLE_TEMPLATE = "http://servicodados.ibge.gov.br/api/v1/dadosintegrados?resp=valores&pesq=1038&var=%d&ag=%dxxxxxx&tiporetorno=json"

_COMPRESSION_TABLE = {
    "A": ",0",
    "B": ",1",
    "C": ",-1",
    "D": ",2",
    "E": ",-2",
    "F": ",3",
    "G": ",-3",
    "H": ",4",
    "I": ",-4",
    "J": ",5",
    "K": ",-5",
    "L": ",6",
    "M": ",-6",
    "N": ",7",
    "O": ",-7",
    "P": ",8",
    "Q": ",-8",
    "R": ",9",
    "S": ",-9",
}

# Available shapes, found in:
# http://www.censo2010.ibge.gov.br/apps/areaponderacao/js/dic.js
_SHAPES = {
    4300604:"Alvorada",
    3501608:"Americana",
    1500800:"Ananindeua",
    5201108:"Anápolis",
    5201405:"Aparecida de Goiânia",
    2800308:"Aracaju",
    2700300:"Arapiraca",
    3503208:"Araraquara",
    3505708:"Barueri",
    3506003:"Bauru",
    1501402:"Belém",
    3300456:"Belford Roxo",
    3106200:"Belo Horizonte",
    3106705:"Betim",
    4202404:"Blumenau",
    1400100:"Boa Vista",
    5300108:"Brasília",
    2905701:"Camaçari",
    2504009:"Campina Grande",
    3509502:"Campinas",
    5002704:"Campo Grande",
    3301009:"Campos dos Goytacazes",
    4304606:"Canoas",
    3510609:"Carapicuíba",
    3201308:"Cariacica",
    2604106:"Caruaru",
    4104808:"Cascavel",
    2303709:"Caucaia",
    4305108:"Caxias do Sul",
    4105805:"Colombo",
    3118601:"Contagem",
    3513009:"Cotia",
    4204608:"Criciúma",
    5103403:"Cuiabá",
    4106902:"Curitiba",
    3513801:"Diadema",
    3122306:"Divinópolis",
    5003702:"Dourados",
    3301702:"Duque de Caxias",
    3515004:"Embu das Artes",
    2910800:"Feira de Santana",
    4205407:"Florianópolis",
    2304400:"Fortaleza",
    4108304:"Foz do Iguaçu",
    3516200:"Franca",
    5208707:"Goiânia",
    3127701:"Governador Valadares",
    4309209:"Gravataí",
    3518701:"Guarujá",
    3518800:"Guarulhos",
    3519071:"Hortolândia",
    2105302:"Imperatriz",
    3520509:"Indaiatuba",
    3131307:"Ipatinga",
    3301900:"Itaboraí",
    2914802:"Itabuna",
    3522505:"Itapevi",
    3523107:"Itaquaquecetuba",
    2607901:"Jaboatão dos Guararapes",
    3524402:"Jacareí",
    2507507:"João Pessoa",
    4209102:"Joinville",
    2918407:"Juazeiro",
    2307304:"Juazeiro do Norte",
    3136702:"Juiz de Fora",
    3525904:"Jundiaí",
    3526902:"Limeira",
    4113700:"Londrina",
    3302403:"Macaé",
    1600303:"Macapá",
    2704302:"Maceió",
    3302502:"Magé",
    1302603:"Manaus",
    1504208:"Marabá",
    2307650:"Maracanaú",
    3529005:"Marília",
    4115200:"Maringá",
    3529401:"Mauá",
    3530607:"Mogi das Cruzes",
    3143302:"Montes Claros",
    2408003:"Mossoró",
    2408102:"Natal",
    3303302:"Niterói",
    3303500:"Nova Iguaçu",
    4313409:"Novo Hamburgo",
    2609600:"Olinda",
    3534401:"Osasco",
    1721000:"Palmas",
    2403251:"Parnamirim",
    2610707:"Paulista",
    4314407:"Pelotas",
    2611101:"Petrolina",
    3303906:"Petrópolis",
    3538709:"Piracicaba",
    4119905:"Ponta Grossa",
    4314902:"Porto Alegre",
    1100205:"Porto Velho",
    3541000:"Praia Grande",
    3541406:"Presidente Prudente",
    2611606:"Recife",
    3154606:"Ribeirão das Neves",
    3543402:"Ribeirão Preto",
    1200401:"Rio Branco",
    3304557:"Rio de Janeiro",
    4315602:"Rio Grande",
    5107602:"Rondonópolis",
    2927408:"Salvador",
    3157807:"Santa Luzia",
    4316907:"Santa Maria",
    1506807:"Santarém",
    3547809:"Santo André",
    3548500:"Santos",
    3548708:"São Bernardo do Campo",
    3548906:"São Carlos",
    3304904:"São Gonçalo",
    3305109:"São João de Meriti",
    4216602:"São José",
    3549805:"São José do Rio Preto",
    3549904:"São José dos Campos",
    4125506:"São José dos Pinhais",
    4318705:"São Leopoldo",
    2111300:"São Luís",
    3550308:"São Paulo",
    3551009:"São Vicente",
    3205002:"Serra",
    3167202:"Sete Lagoas",
    3552205:"Sorocaba",
    3552403:"Sumaré",
    3552502:"Suzano",
    3552809:"Taboão da Serra",
    3554102:"Taubaté",
    2211001:"Teresina",
    3170107:"Uberaba",
    3170206:"Uberlândia",
    5108402:"Várzea Grande",
    4323002:"Viamão",
    3205200:"Vila Velha",
    3205309:"Vitória",
    2933307:"Vitória da Conquista",
    3306305:"Volta Redonda"
}

class Variable(IntEnum):
    MONTHLY_AVERAGE_INCOME = 12786

def _extract(s):
    """
    Extract data using IBGE custom "compression" algorithm.

    """

    s = s.replace("0E+", "0e+")
    spt = s.split()
    ret = []

    for s in spt:
        s_conv = ''
        for ch in s:
            if ch in _COMPRESSION_TABLE:
                s_conv += _COMPRESSION_TABLE[ch]
            else:
                s_conv += ch

        data = s_conv.split(",")
        norm = float(data[0])

        shape = []
        lng = 0.0
        lat = 0.0
        for i in range(1, len(data)-1, 2):
            lng += float(data[i]) / norm
            lat += float(data[i+1]) / norm
            shape.append((lat, lng))
        ret.append(shape)
    return ret

def _name_code(name):
    return strip_diacritics(name.lower().replace(" ", "_"))

def download_shapes():
    dest = unissono.datasets.DATASETS_DIR / "censo_ibge_2010" / "shapes"
    dest.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading area shapes")
    for code, name in _SHAPES.items():
        name_code = _name_code(name)
        logger.info("Downloading shape code= %s output= %s", code, name_code)
        shape_data = unissono.datasets.download_content(_API_SHAPE_TEMPLATE%(code))
        shape_data = shape_data.decode("utf-8").replace(":", " : ")
        shape_data = yaml.load(shape_data)
        for ug, ug_data in shape_data["shapes"].items():
            shape_data["shapes"][ug] = _extract(ug_data)
        fname = dest / ("%s.json"%(name_code))
        with fname.open("w") as f:
            f.write(json.dumps(shape_data))

def download_variable(var_id):
    """
    Download specific variable from IBGE dataset.

    http://servicodados.ibge.gov.br/api/v1/dadosintegrados?resp=variaveis&per=2010&pesq=1038&tiporetorno=json&alvo=agsnap
    """

    dest = unissono.datasets.DATASETS_DIR / "censo_ibge_2010" / "variables"
    dest.mkdir(parents=True, exist_ok=True)

    for code, name in _SHAPES.items():
        name_code = _name_code(name)
        logging.info("Downloading variable var_id= %d name= %s"%(var_id, name_code))
        var_data = unissono.datasets.download_content(_API_VARIABLE_TEMPLATE%(var_id, code))
        var_data = var_data.decode("utf-8").replace(":", " : ")
        var_data = yaml.load(var_data)
        fname = dest / ("%d_%s_%d.json"%(code, name_code, var_id))
        with fname.open("w") as f:
            f.write(json.dumps(var_data))

def merge_data(var_id):
    dest = unissono.datasets.DATASETS_DIR / "censo_ibge_2010" / "merged"
    dest.mkdir(parents=True, exist_ok=True)

    shape_dir = unissono.datasets.DATASETS_DIR / "censo_ibge_2010" / "shapes"
    var_dir = unissono.datasets.DATASETS_DIR / "censo_ibge_2010" / "variables"
    res = {}
    for code, name in _SHAPES.items():
        name_code = _name_code(name)

        shape_content = (shape_dir / ("%s.json"%(name_code))).open("r").read()
        shape_data = json.loads(shape_content)
        var_content = (var_dir / ("%d_%s_%d.json"%(code, name_code, var_id))).open("r").read()
        var_raw_data = json.loads(var_content)


        var_data = {}
        for k, vd in var_raw_data.items():
            var_data[vd["UG"]] = float(vd["V"].replace(",", "."))

        for ug, shape in shape_data["shapes"].items():
            res[ug] = {
                "shapes": shape,
                "value": var_data[ug],
                "name": name_code,
            }

    with (dest / ("%s.json"%(int(var_id)))).open("w") as f:
        f.write(json.dumps(res))

def download():
    """
    TODO: use microdata and real shape files instead
    """
    dest = unissono.datasets.DATASETS_DIR / "censo_ibge_2010"
    if dest.exists():
       logger.info("IBGE directory already exists, skipping")
       return

    download_shapes()
    download_variable(Variable.MONTHLY_AVERAGE_INCOME)
    merge_data(Variable.MONTHLY_AVERAGE_INCOME)

def load_variable(var_id):
    dest = unissono.datasets.DATASETS_DIR / "censo_ibge_2010" / "merged" / ("%d.json"%(var_id))
    data = json.loads(dest.open("r").read())
    return data

def in_path(shp, x, y):
    """
    Return true if given point is inside shape
    """
    n = len(shp)
    inside = False

    p1x, p1y = shp[0]
    for i in range(n+1):
        p2x, p2y = shp[i % n]
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xinters = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside

def get_variable(var_data, x, y):
    """
    TODO: horrible algorithm, use QuadTree instead
    """
    for code, data in var_data.items():
        for poly in data["shapes"]:
            if in_path(poly, x, y):
                return (code, data["value"])
    return None
