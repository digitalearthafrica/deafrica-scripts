// s5p_o3.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "O3",
          "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "O3",      bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "dataMask", bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}

function evaluatePixel(sample) {
  return {

    O3: [
      isNaN(sample.O3)
        ? -9999
        : sample.O3
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