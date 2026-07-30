// s5p_hcho.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "HCHO",
          "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "HCHO",    bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "dataMask", bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}

function evaluatePixel(sample) {
  return {

    HCHO: [
      isNaN(sample.HCHO)
        ? -9999
        : sample.HCHO
    ],

    dataMask: [
      isNaN(sample.dataMask)
        ? 255
        : sample.dataMask
    ]
  };
}

// Store provenance information for STAC generation
function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
  outputMetadata.userData = {
    tiles: scenes.tiles,
    serviceVersion: inputMetadata.serviceVersion
  };
}