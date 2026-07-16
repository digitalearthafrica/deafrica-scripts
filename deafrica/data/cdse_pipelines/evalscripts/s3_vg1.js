// s3_vg1.js
//VERSION=3
//

const BANDS = [
    { name: "B0_VG1", sampleType: "INT16", scalingFactor: 10000, offset: 0, nodataValue: 32767 },
    { name: "B2_VG1", sampleType: "INT16", scalingFactor: 10000, offset: 0, nodataValue: 32767 },
    { name: "B3_VG1", sampleType: "INT16", scalingFactor: 10000, offset: 0, nodataValue: 32767 },
    { name: "MIR_VG1", sampleType: "INT16", scalingFactor: 10000, offset: 0, nodataValue: 32767 },
    { name: "NDVI_VG1", sampleType: "INT16", scalingFactor: 10000, offset: 0, nodataValue: 32767 },
    { name: "TOA_NDVI_VG1", sampleType: "INT16", scalingFactor: 10000, offset: 0, nodataValue: 32767 },
    { name: "OG_VG1", sampleType: "UINT8", scalingFactor: 1, offset: 0, nodataValue: 255 },
    { name: "SAA_VG1", sampleType: "INT16", scalingFactor: 1, offset: 0, nodataValue: 32767 },
    { name: "SZA_VG1", sampleType: "UINT8", scalingFactor: 1, offset: 0, nodataValue: 255 },
    { name: "VAA_VG1", sampleType: "INT16", scalingFactor: 1, offset: 0, nodataValue: 32767 },
    { name: "VZA_VG1", sampleType: "UINT8", scalingFactor: 1, offset: 0, nodataValue: 255 },
    { name: "WVG_VG1", sampleType: "UINT8", scalingFactor: 1, offset: 0, nodataValue: 255 },
    { name: "AG_VG1", sampleType: "UINT8", scalingFactor: 1, offset: 0, nodataValue: 255 },
];

function setup() {
    return {
        input: [{
            bands: BANDS.map(band => band.name),
        }],
        output: BANDS.map(band => ({
            id: band.name,
            bands: 1,
            sampleType: band.sampleType,
            nodataValue: band.nodataValue
        }))
    };
}

function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
    outputMetadata.userData = {
        tiles: scenes.tiles,
        serviceVersion: inputMetadata.serviceVersion
    };
}

function evaluatePixel(sample) {
    var outputDict = {};
    for (let band of BANDS) {
        outputDict[band.name] = [
            isNaN(sample[band.name])
                ? band.nodataValue
                : Math.round(sample[band.name] * band.scalingFactor - band.offset)
        ];
    }
    return outputDict
}