// s3_lfr.js
//VERSION=3
//

function setup() {
  return {
    input: [
      {
        bands: [
          "GIFAPAR",
          "IWV_L",
          "OTCI",
          "RC681",
          "RC865",
          "LQSF",
          "dataMask"
        ],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "GIFAPAR",  bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "IWV_L",    bands: 1, sampleType: "UINT8",   nodataValue: 255 },
      { id: "OTCI",     bands: 1, sampleType: "UINT16",   nodataValue: 65535 },
      { id: "RC681",    bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "RC865",    bands: 1, sampleType: "UINT16",  nodataValue: 65535 },
      { id: "LQSF",     bands: 1, sampleType: "FLOAT32", nodataValue: -1 },
      { id: "dataMask", bands: 1, sampleType: "UINT8",   nodataValue: 255 }
    ]
  };
}

function evaluatePixel(sample) {
  return {

    // Fraction (0-1) -> uint16 scaled by 10000
    GIFAPAR: [
      isNaN(sample.GIFAPAR)
        ? 65535
        : Math.round(sample.GIFAPAR * 10000)
    ],

    // Already integer
    IWV_L: [
      isNaN(sample.IWV_L)
        ? 255
        : sample.IWV_L
    ],

    // Index -> uint16 scaled by 10000
    OTCI: [
      isNaN(sample.OTCI)
        ? 65535
        : Math.round(sample.OTCI * 10000)
    ],

    // Reflectance -> uint16 scaled by 10000
    RC681: [
      isNaN(sample.RC681)
        ? 65535
        : Math.round(sample.RC681 * 10000)
    ],

    RC865: [
      isNaN(sample.RC865)
        ? 65535
        : Math.round(sample.RC865 * 10000)
    ],

    // Float quality flag
    LQSF: [
      isNaN(sample.LQSF)
        ? -1
        : sample.LQSF
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