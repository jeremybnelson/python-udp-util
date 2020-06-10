# New libs
# from settings import settings
from office365.runtime.auth.UserCredential import UserCredential
from office365.sharepoint.caml_query import CamlQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.file_creation_information import FileCreationInformation
from office365.sharepoint.file import File
import os


# Sharepoint functions
def download_file(context):
    path = "../ref_docs/lkpEntityLocation.xlsx"
    # path = "../ref_docs/Test.docx"
    response = File.open_binary(context, "/teams/UDPDevTeam/MasterData/dev/access/lkpEntityLocation.xlsx")
    response.raise_for_status()
    with open(path, "wb") as local_file:
        local_file.write(response.content)


def upload_file(context):
    path = "../ref_docs/Test.docx"
    with open(path, 'rb') as content_file:
        file_content = content_file.read()

    list_title = "test_upload.docx"
    target_folder = context.web.lists.get_by_title(list_title).rootFolder
    info = FileCreationInformation()
    info.content = file_content
    info.url = os.path.basename(path)
    info.overwrite = True
    target_file = target_folder.files.add(info)
    context.execute_query()
    print("File url: {0}".format(target_file.properties["ServerRelativeUrl"]))

def read_folder_and_files(context, list_title):
    """Read a folder example"""
    list_obj = context.web.lists.get_by_title(list_title)
    folder = list_obj.rootFolder
    context.load(folder)
    context.execute_query()
    print("List url: {0}".format(folder.properties["ServerRelativeUrl"]))

    files = folder.files
    context.load(files)
    context.execute_query()
    for cur_file in files:
        print("File name: {0}".format(cur_file.properties["Name"]))

    folders = context.web.folders
    context.load(folders)
    context.execute_query()
    for folder in folders:
        print("Folder name: {0}".format(folder.properties["Name"]))


def read_all_folders(context):
    # ctx.load()
    context_2 = context
    folders = context.web.folders
    context.load(folders)
    context.execute_query()

    # context.load(folders)
    masterdata_folder = folders.get_by_url('/teams/UDPDevTeam/MasterData')
    context_2.load(masterdata_folder)
    context_2.execute_query()
    for folder in masterdata_folder.folders:
        print("Folder name: {0}".format(folder.properties["Name"]))

# Everything starts here
def main():
    sdlc = 'dev'
    site_url = \
        'https://alterramtnco.sharepoint.com/:x:/r/teams/UDPDevTeam/MasterData/dev/access/lkpEntityLocation.xlsx?d=' \
        'w6788e8cf8c984cb3a5da58b7fe09b45c&csf=1&web=1&e=XKVdtd'
    # site_url = 'https://alterramtnco.sharepoint.com/:f:/r/teams/UDPDevTeam/MasterData/dev?csf=1&web=1&e=wlVsxo'
    # site_url = 'https://alterramtnco.sharepoint.com/:f:/r/teams/UDPDevTeam/MasterData/dev?csf=1&web=1&e=oZh6g1'

    # MasterData > Dev
    # site_url = 'https://alterramtnco.sharepoint.com/:f:/r/teams/UDPDevTeam/MasterData/dev?csf=1&web=1&e=Hb8FYE'
    # site_url = 'https://alterramtnco.sharepoint.com/:w:/r/teams/UDPDevTeam/MasterData/dev/Test.docx' \
    #            '?d=w0ad8c687dc9a4725a98b1d18953eb0ae&csf=1&web=1&e=UWAWMa'
    site_url = 'https://alterramtnco.sharepoint.com/teams/UDPDevTeam'
    # site_url = 'https://alterramtnco.sharepoint.com/:f:/r/teams/UDPDevTeam'
    ctx = ClientContext.connect_with_credentials(site_url, UserCredential('jeremy.nelson@alterramtnco.com'
                                                                          , '4@Potatoes'))

    # web = ctx.web
    # ctx.load(web)
    # ctx.execute_query()
    # print("Web title: {0}".format(web.properties['Title']))

    # list_obj = ctx.web.lists.get_by_title('MasterData')
    # folder = list_obj.rootFolder
    # ctx.load(folder)
    # ctx.execute_query()
    # print("List url: {0}".format(folder.properties["ServerRelativeUrl"]))
    # ServerRelativeUrl = "{0}".format(folder.properties["ServerRelativeUrl"])
    # upload_file(ctx)
    # 'https://alterramtnco.sharepoint.com/:f:/r/teams/UDPDevTeam/MasterData/dev?csf=1&web=1&e=DRKVOw'
    read_folder_and_files(ctx, 'Form Templates')

    # read_all_folders(ctx)
    # print(ctx.get_lists())
    download_file(ctx)



if __name__ == '__main__':
    main()
