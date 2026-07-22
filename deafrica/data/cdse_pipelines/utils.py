from importlib.resources import files


def load_evalscript(path):
    return files("deafrica.data.cdse_pipelines").joinpath(path).read_text()
