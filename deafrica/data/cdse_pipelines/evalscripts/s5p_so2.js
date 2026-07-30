// s5p_so2.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "SO2",
          "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "SO2",     bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "dataMask", bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}

function evaluatePixel(sample) {
  return {

    SO2: [
      isNaN(sample.SO2)
        ? -9999
        : sample.SO2
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