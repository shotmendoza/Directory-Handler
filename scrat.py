from src.base import Folder, FileList

path = f"/Volumes/MendozaHub/root/Entertainment/3. Documentaries/"


f = Folder(path)
vids = f.index_files(file_ext=".mp4", recurse=True)
playlist = FileList(vids)

playlist.shuffle(10)
