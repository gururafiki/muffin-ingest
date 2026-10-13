"""Constants the companies family shares. Its grid is the price lane's (`prices.partitions`)."""

#: HOW MANY COMPANIES ONE RUN COVERS, a TIME budget. A company costs at most one summary and two
#: statement requests at the price lane's 1.4 s, so 100 companies are at most ~7 minutes, and a
#: weekly visit where only the summary is due ~2.5. The documents are small (20-70 KB a summary,
#: 120-170 KB a timeseries body), so memory is not what binds.
COMPANY_PARTITIONS_PER_RUN = 100

#: How many days a full rotation takes: each company's summary is due weekly.
ROTATION_DAYS = 7
