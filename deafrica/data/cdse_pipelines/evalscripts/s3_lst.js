// s3_lst.js
//VERSION=3
//

const BANDS = [
    { name: "LST", sampleType: "UINT16", scalingFactor: 100, offset: 0, nodataValue: 65535 },
    { name: "LST_uncertainty", sampleType: "UINT16", scalingFactor: 100, offset: 0, nodataValue: 65535 },
    { name: "NDVI", sampleType: "INT16", scalingFactor: 10000, offset: 0, nodataValue: 32767 },
    { name: "dataMask", sampleType: "UINT8", scalingFactor: 1, offset: 0, nodataValue: 255 },
    { name: "CONFIDENCE", sampleType: "UINT16", scalingFactor: 1, offset: 0, nodataValue: 65535 },
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
        })),
        mosaicking: "ORBIT"
    };
}

function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
    if (scenes.orbits && scenes.orbits.length > 0) {
        outputMetadata.userData = { scenes: scenes.orbits };
    }
}

function evaluatePixel(samples, scenes) {
    var outputDict = {};
    // LST has two overpasses a day, day and night. Takes the newest pass.
    const index = samples.length > 0 ? 0 : -1;
    for (let band of BANDS) {
        const id = band.name;
        if (band.name === "dataMask") {
            outputDict[id] = [index !== -1 && !isNaN(samples[index].LST)]
        } else if (index == -1) {
            outputDict[id] = [band.nodataValue]
        } else if (isNaN(samples[index][band.name])) {
            outputDict[id] = [band.nodataValue]
        } else {
            outputDict[id] = [samples[index][band.name] * band.scalingFactor - band.offset]
        }
    }
    return outputDict
}