# Drinking Water Database

### How We Defined Our Dataset

Because of recent blowout activity in Louisiana, ongoing earthquakes in Oklahoma due to fracking, and the public outcry over deteriorating water quality, we thought of trying to see how water is determined to be "safe".  Defining impurities in drinking water, how they are measured, and the "acceptable" consumption was eye opening.  We used sources from the EPA, a fracking website, and chemical abstracts service numbers to define and connect our data.  what was truly terrifying was the tables in the EPA, we found no connections in the defined chemicals to the chemicals listed in the fracking data.  Although the overall analysis of our data is not within the scope of this project, we had to note that shocking revelation.

#### Extract

We looked through the internet to find content surrounding water quality, including common contaminates, government reported acceptable levels of impurities, and chemicals used in hydraulic fracturing.  For the EPA tables of chemicals for human health and aquatic life, we used the Pandas_HTML commands to extract the tables.  The content in these tables was fairly clean when they transferred into the DataFrame.  The FracFocus table also was extracted using the Pandas_HTML command. 

#### Transform

The data coming out of the tables from the EPA was fairly clean, there were no date conversions, erroneous or duplicative data points that required any additional manipulation.  Having the CAS number as a unique identifier across all tables was very helpful.  Within the FracFocus table, the only additional manipulation was parsing the trailing "-##-#" numbers so they would match across all tables.

#### Load

We used the MongoDB for loading our data.  We found this would be easiest, given the range of data points across the diverse data sets we were working with.