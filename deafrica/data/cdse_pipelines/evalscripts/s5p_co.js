// s5p_co.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "CO",
          "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "CO",      bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "dataMask", bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}

function evaluatePixel(sample) {
  return {

    CO: [
      isNaN(sample.CO)
        ? -9999
        : sample.CO
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