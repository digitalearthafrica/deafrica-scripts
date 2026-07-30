// s5p_aer_ai.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "AER_AI_340_380",
          "AER_AI_354_388",
          "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "AER_AI_340_380", bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "AER_AI_354_388", bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "dataMask",       bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}

function evaluatePixel(sample) {
  return {

    // Unitless aerosol index - stored as float, no scaling
    AER_AI_340_380: [
      isNaN(sample.AER_AI_340_380)
        ? -9999
        : sample.AER_AI_340_380
    ],

    AER_AI_354_388: [
      isNaN(sample.AER_AI_354_388)
        ? -9999
        : sample.AER_AI_354_388
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