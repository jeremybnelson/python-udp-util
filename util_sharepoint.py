import sharepy


# (1) Authenticate
s = sharepy.connect(site=
                    'https://alterramtnco.sharepoint.com/sites/AlterraDataTeam/MasterData/Forms/AllItems.aspx?id=%2Fsites%2FAlterraDataTeam%2FMasterData%2Fdev'
                    , username='jeremy.nelson'
                    , password='4@Poopdoop')
r = s.get('https://alterramtnco.sharepoint.com/sites/AlterraDataTeam/MasterData/Forms/AllItems.aspx?id=%2Fsites%2FAlterraDataTeam%2FMasterData%2Fdev')
