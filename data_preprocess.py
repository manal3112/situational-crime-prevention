import pandas as pd

def get_aggregate_data(year_range = range(15,21), month_range = range(1,13), agg_type = 'mean', min_population_threshold = 1000.0):
    # year_range = range(6,11) #Should be between 1980-2020
    # month_range = range(1,13) #Should be between 1-12

    valid_offense_codes = ['01A', '01B', '02', '03', '04', '05', '06', '07', '08', '09', '10', 
                            '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', 
                            '21', '22', '23', '24', '25', '26', '27', '28', '29']

    dfs = []
    for year in year_range:
        if year in range(80,100): #1980-1999
            f = 'dataset/ASR_19{}.csv'.format(year)
        if year in range(0,10): #2000-2009
            f = 'dataset/ASR_200{}.csv'.format(year)
        if year in range(10,21): #2010-2020
            f = 'dataset/ASR_20{}.csv'.format(year)

        year_df = pd.read_csv(f, low_memory=False)
        df = year_df[year_df['Year'].isin([year])][year_df['Month'].isin(month_range)]

        # Total Arrests 
        df["Total_Males"]   = df.iloc[:, 25:47].sum(axis = 1)
        df["Total_Females"] = df.iloc[:, 47:69].sum(axis = 1)
        df["Arrests"]       = df["Total_Males"] + df["Total_Females"]

        # Merge Age Groups
        df["Under-10"]      = df.iloc[:, 25] + df.iloc[:, 47]
        df["10-19"]         = df.iloc[:, 26:33].sum(axis = 1) + df.iloc[:, 48:55].sum(axis = 1) 
        df["20-29"]         = df.iloc[:, 33:39].sum(axis = 1) + df.iloc[:, 55:61].sum(axis = 1)
        df["30-39"]         = df.iloc[:, 39:41].sum(axis = 1) + df.iloc[:, 61:63].sum(axis = 1)
        df["40-49"]         = df.iloc[:, 41:43].sum(axis = 1) + df.iloc[:, 63:65].sum(axis = 1)
        df["50-59"]         = df.iloc[:, 43:45].sum(axis = 1) + df.iloc[:, 65:67].sum(axis = 1)
        df["Over-60"]       = df.iloc[:, 45:47].sum(axis = 1) + df.iloc[:, 67:69].sum(axis = 1)

        # Merge Ethnicity and Race 
        df["White"]         = df["Adult-White"] + df["Juvenile-White"]
        df["Black"]         = df["Adult-Black"] + df["Juvenile-Black"]
        df["Indian"]        = df["Adult-Indian"] + df["Juvenile-Indian"]
        df["Asian"]         = df["Adult-Asian"] + df["Juvenile-Asian"]
        df["Hispanic"]      = df["Adult-Hispanic"] + df["Juvenile-Hispanic"]
        df["Non-Hispanic"]  = df["Adult-Non-Hispanic"] + df["Juvenile-Non-Hispanic"]

        #Clean Offense codes
        df["Offense_Code"]  = df["Offense_Code"].str.rstrip()

        df = df[df["Offense_Code"].isin(valid_offense_codes)]

        df.drop(columns     = [ 'State_Code', 'Division', 'County', #'Group', 'MSA','State_Name', # 'ORI_Code', 'Agency_Name',
                                'Adult-White', 'Adult-Black', 'Adult-Indian', 'Adult-Asian', 'Juvenile-White', 'Juvenile-Black', 'Juvenile-Indian', 
                                'Juvenile-Asian', 'Adult-Hispanic', 'Adult-Non-Hispanic', 'Juvenile-Hispanic', 'Juvenile-Non-Hispanic',
                                'Male-Under-10', 'Male-10-12', 'Male-13-14', 'Male-15', 'Male-16',
                                'Male-17', 'Male-18', 'Male-19', 'Male-20', 'Male-21', 'Male-22',
                                'Male-23', 'Male-24', 'Male-25-29', 'Male-30-34', 'Male-35-39',
                                'Male-40-44', 'Male-45-49', 'Male-50-54', 'Male-55-59', 'Male-60-64', 'Male-Over-64', 
                                'Female-Under-10', 'Female-10-12', 'Female-13-14','Female-15', 'Female-16', 
                                'Female-17', 'Female-18', 'Female-19','Female-20', 'Female-21', 'Female-22', 
                                'Female-23', 'Female-24', 'Female-25-29', 'Female-30-34', 'Female-35-39', 
                                'Female-40-44','Female-45-49', 'Female-50-54', 'Female-55-59', 'Female-60-64', 'Female-Over-64']
                                , inplace = True)

        df.drop(columns = ['Year', 'Month'], inplace = True)

        gdf = df.groupby(['ORI_Code', 'Agency_Name', 'Suburban', 'Offense_Code']) \
                .agg({'Population_for_Agency':'mean', 'Total_Males':'sum', 'Total_Females':'sum','Arrests':'sum', 'Under-10':'sum',
                    '10-19':'sum', '20-29':'sum', '30-39':'sum', '40-49':'sum', '50-59':'sum', 'Over-60':'sum', 
                    'White':'sum', 'Black':'sum', 'Indian':'sum', 'Asian':'sum', 'Hispanic':'sum', 'Non-Hispanic':'sum'})
        gdf.reset_index(inplace = True)
        gdf.set_index(['ORI_Code', 'Agency_Name', 'Suburban', 'Offense_Code', 'Population_for_Agency'])

        pgdf = gdf.pivot_table(index = ['ORI_Code', 'Agency_Name', 'Suburban','Population_for_Agency'], columns = 'Offense_Code')
        pgdf.columns = list(map("__".join, [t[::-1] for t in pgdf.columns]))
        pgdf.reset_index(inplace = True)

        # Filter agencies with population greater than min_population_threshold
        pgdf = pgdf[pgdf["Population_for_Agency"] >= min_population_threshold]
        pgdf.reset_index(inplace = True, drop = True)

        # Normalize by populaiton and take percentage
        pgdf.iloc[:,4:] = pgdf.iloc[:,4:].div(pgdf["Population_for_Agency"], axis = 0).apply(lambda x: x*100)

        dfs.append(pgdf)

    ydfs = pd.concat(dfs)
    ydfs.reset_index(drop = True, inplace = True)

    final_df = ydfs.groupby(['ORI_Code', 'Agency_Name', 'Suburban']).agg(agg_type)
    final_df.reset_index(inplace = True)

    result_file = "rdd_{}_{}.csv".format(year_range[0], year_range[-1])
    final_df.to_csv(result_file, index = False)

    return result_file
