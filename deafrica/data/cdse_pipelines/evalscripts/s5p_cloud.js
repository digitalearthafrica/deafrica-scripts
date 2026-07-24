// s5p_cloud.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "CLOUD_BASE_PRESSURE",
          "CLOUD_TOP_PRESSURE",
          "CLOUD_BASE_HEIGHT",
          "CLOUD_TOP_HEIGHT",
          "CLOUD_OPTICAL_THICKNESS",
          "CLOUD_FRACTION",
          "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "CLOUD_BASE_PRESSURE",    bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "CLOUD_TOP_PRESSURE",     bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "CLOUD_BASE_HEIGHT",      bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "CLOUD_TOP_HEIGHT",       bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "CLOUD_OPTICAL_THICKNESS", bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "CLOUD_FRACTION",         bands: 1, sampleType: "FLOAT32", nodataValue: -9999 },
      { id: "dataMask",               bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}

function evaluatePixel(sample) {
  return {

    CLOUD_BASE_PRESSURE: [
      isNaN(sample.CLOUD_BASE_PRESSURE)
        ? -9999
        : sample.CLOUD_BASE_PRESSURE
    ],

    CLOUD_TOP_PRESSURE: [
      isNaN(sample.CLOUD_TOP_PRESSURE)
        ? -9999
        : sample.CLOUD_TOP_PRESSURE
    ],

    CLOUD_BASE_HEIGHT: [
      isNaN(sample.CLOUD_BASE_HEIGHT)
        ? -9999
        : sample.CLOUD_BASE_HEIGHT
    ],

    CLOUD_TOP_HEIGHT: [
      isNaN(sample.CLOUD_TOP_HEIGHT)
        ? -9999
        : sample.CLOUD_TOP_HEIGHT
    ],

    CLOUD_OPTICAL_THICKNESS: [
      isNaN(sample.CLOUD_OPTICAL_THICKNESS)
        ? -9999
        : sample.CLOUD_OPTICAL_THICKNESS
    ],

    CLOUD_FRACTION: [
      isNaN(sample.CLOUD_FRACTION)
        ? -9999
        : sample.CLOUD_FRACTION
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