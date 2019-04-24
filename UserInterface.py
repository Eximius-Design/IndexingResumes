class UserInterface:
    def form_html(DataFrame):
        print("DataFrame.columns()",DataFrame.columns)
        # File_Names=DataFrame['File Name']
        # Skills_Found=DataFrame['SkillsFound']
        # Engineer_Type=DataFrame['Engineer Type']
        # Experience=DataFrame['Experience']
        # Drive_Links=DataFrame['DriveLinks']
        html=""
        for index, row in DataFrame.iterrows():
            File_Name=row['File Name']
            Skills_Found=row['SkillsFound']
            Engineer_Type=row['Engineer Type']
            Experience=row['Experience']
            Drive_Links=row['DriveLinks']
            html=html+"<h3>"+File_Name+"</h3>"+"Years Of Experience:"+Experience+"<br>"+"Engineer Type:"+Engineer_Type+"<br>"\
                 +"Skills Found:"+Skills_Found+"<br>"+"Link:"+Drive_Links+"</br>"

        return html
        # return DataFrame