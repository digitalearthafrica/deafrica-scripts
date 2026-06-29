// s1_rtc.js
// VERSION=3

function setup() {
  return {
    input: [
      {
        bands: ["VV", "VH", "scatteringArea", "localIncidenceAngle", "shadowMask", "dataMask"],
        metadata: ["bounds"]
      }
    ],
    output: [
      { id: "VV", bands: 1, sampleType: "FLOAT32", nodataValue: NaN },
      { id: "VH", bands: 1, sampleType: "FLOAT32", nodataValue: NaN },
      { id: "AREA", bands: 1, sampleType: "FLOAT32", nodataValue: NaN },
      { id: "ANGLE", bands: 1, sampleType: "UINT8", nodataValue: 255 },
      { id: "MASK", bands: 1, sampleType: "UINT8", nodataValue: 0 }
    ]
  };
}

function evaluatePixel(samples) {
  return {
    VV: [samples.VV],
    VH: [samples.VH],
    AREA: [samples.scatteringArea],
    ANGLE: [samples.dataMask == 0 ? 255 : samples.localIncidenceAngle],
    MASK: [samples.shadowMask == 1 ? 2 : samples.dataMask]
  };
}

function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
  outputMetadata.userData = {
    "tiles": scenes.tiles,
    "serviceVersion": inputMetadata.serviceVersion
  };
}
