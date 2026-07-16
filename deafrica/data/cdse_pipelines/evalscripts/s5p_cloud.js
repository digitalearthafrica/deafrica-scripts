// s5p_ch4.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "CH4",
          "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "CH4",     bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "dataMask", bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}

function evaluatePixel(sample) {
  return {

    CH4: [
      isNaN(sample.CH4)
        ? -9999
        : sample.CH4
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