# ML Pipeline for predicting optimal Hydropower plant locations

## How it's done?

For project documentation check out [Hydropower Site Suitability Doc (PDF)](./Hydropower%20Site%20Suitabilty%20Doc.pdf)

## Data extraction for training

Requests for satellite data use the dataspce.copernicus API.

> **_IMPORTANT:_**
> For data extraction using Sentinel API make sure to enter your OAuth client id and client secret from the dataspace.copernicus.eu dashboard.

The shape file needed for european river locations is from https://www.hydrosheds.org/products/hydrorivers
Place the downloaded shape file in the `data/` folder

The hydropower plant database is downloaded from https://figshare.com/articles/dataset/Global_Hydropower_Database_GHD_/11283758
