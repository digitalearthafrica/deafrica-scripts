// s3_wfr.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "A865", "ADG443_NN",
          "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09",
          "B10", "B11", "B12", "B16", "B17", "B18", "B21",
          "CHL_NN", "CHL_OC4ME", "IWV_W", "KD490_M07", "PAR", "T865",
          "TSM_NN", "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "A865",      bands: 1, sampleType: "INT16",   nodataValue: 32767 },
      { id: "ADG443_NN", bands: 1, sampleType: "INT16",   nodataValue: 32767 },
      { id: "B01",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B02",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B03",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B04",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B05",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B06",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B07",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B08",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B09",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B10",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B11",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B12",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B16",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B17",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B18",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "B21",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "CHL_NN",    bands: 1, sampleType: "INT16",   nodataValue: 32767 },
      { id: "CHL_OC4ME", bands: 1, sampleType: "INT16",   nodataValue: 32767 },
      { id: "IWV_W",     bands: 1, sampleType: "UINT8",   nodataValue: 255 },
      { id: "KD490_M07", bands: 1, sampleType: "INT16",   nodataValue: 32767 },
      { id: "PAR",       bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "T865",      bands: 1, sampleType: "INT16",   nodataValue: 32767 },
      { id: "TSM_NN",    bands: 1, sampleType: "INT16",   nodataValue: 32767 },
      { id: "dataMask",  bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}
 
// x10000 -> INT16 with 32767 nodata
function i16(v) {
  return isNaN(v) ? 32767 : Math.round(v * 10000);
}
 
// x10000 -> UINT16 with 65535 nodata
function u16(v) {
  return isNaN(v) ? 65535 : Math.round(v * 10000);
}
 
function evaluatePixel(sample) {
  return {
    A865:       [i16(sample.A865)],
    ADG443_NN:  [i16(sample.ADG443_NN)],
    B01:        [u16(sample.B01)],
    B02:        [u16(sample.B02)],
    B03:        [u16(sample.B03)],
    B04:        [u16(sample.B04)],
    B05:        [u16(sample.B05)],
    B06:        [u16(sample.B06)],
    B07:        [u16(sample.B07)],
    B08:        [u16(sample.B08)],
    B09:        [u16(sample.B09)],
    B10:        [u16(sample.B10)],
    B11:        [u16(sample.B11)],
    B12:        [u16(sample.B12)],
    B16:        [u16(sample.B16)],
    B17:        [u16(sample.B17)],
    B18:        [u16(sample.B18)],
    B21:        [u16(sample.B21)],
    CHL_NN:     [i16(sample.CHL_NN)],
    CHL_OC4ME:  [i16(sample.CHL_OC4ME)],
    IWV_W:      [isNaN(sample.IWV_W) ? 255 : Math.round(sample.IWV_W)],
    KD490_M07:  [i16(sample.KD490_M07)],
    PAR:        [isNaN(sample.PAR) ? 65535 : Math.round(sample.PAR)],
    T865:       [i16(sample.T865)],
    TSM_NN:     [i16(sample.TSM_NN)],
    dataMask:   [isNaN(sample.dataMask) ? 255 : sample.dataMask]
  };
}
 
// Store provenance information for STAC generation
function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
  outputMetadata.userData = {
    tiles: scenes.tiles,
    serviceVersion: inputMetadata.serviceVersion
  };
}