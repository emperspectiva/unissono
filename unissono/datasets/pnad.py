import logging
import pandas as pd
from tqdm import tqdm

import unissono.datasets

_PNAD_TRABALHO_2017 = "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/2017/Suplementos/Dados/PNADC_022017_educacao_20180816.zip" # noqa
_PNAD_TRABALHO_2017_INPUT = "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/2017/Suplementos/Documentacao/Input_PNADC_trimestral_educacao_20180816.txt" # noqa

logger = logging.getLogger(__name__)


def download():
    dest = unissono.datasets.DATASETS_DIR / "pnad_trabalho_2017"
    logger.info("Downloading PNAD_TRABALHO_2017")
    fname = unissono.datasets.download(_PNAD_TRABALHO_2017, dest)
    logger.info("Downloading SAS input")
    unissono.datasets.download(_PNAD_TRABALHO_2017_INPUT, dest)
    logger.info("Extracting dataset files")
    unissono.datasets.extract_zip(fname.absolute().as_posix(),
                                  dest.absolute().as_posix())


def _parse_sas(fname):
    data_dict = []
    with fname.open("r") as f:
        for line in f:
            line = line.strip()
            if line.startswith("@"):
                spt = line.split(maxsplit=3)
                start = int(spt[0].strip("@"))-1  # 0 index
                col = spt[1]
                sz = int(spt[2].strip("$").strip("."))
                doc = spt[3].strip("/*").strip("*/").strip()
                data_dict.append([start, col, sz, doc])
    return data_dict


def _file_lines(fname):
    lines = 0
    with fname.open("r") as f:
        buf_size = 1024*1024
        buf = f.read(buf_size)
        while buf:
            lines += buf.count("\n")
            buf = f.read(buf_size)
    return lines


def load():
    dest = unissono.datasets.DATASETS_DIR / "pnad_trabalho_2017"
    fname_input = dest / "Input_PNADC_trimestral_educacao_20180816.txt"
    cols = _parse_sas(fname_input)
    fname = dest / "PNADC_022017_educacao_20180815.txt"

    columns = [x[1] for x in cols]
    data_dict = {x[1]: x[3] for x in cols}
    data = []

    with fname.open("r") as f:
        with tqdm(total=_file_lines(fname)) as pbar:
            for line in f:
                row = []
                for start, col, sz, doc in cols:
                    v = line[start:start+sz]
                    row.append(v)
                data.append(row)
                pbar.update(1)
    df = pd.DataFrame(data, columns=columns)
    return df, data_dict
