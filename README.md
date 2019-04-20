# gdrive_backup
Originally written by qss@U2, with slightly different behaviors when there are concurrent tasks.

Automatically backup torrent contents (like U2) to google drive, including raw folder upload and PAR2-verified RAR volume.

## Usage
Run `bootstrap.py` first, which creates `config.ini`.

Backup command:
```
python3 main.py <ContentCategory> <LocalFolderPath>
```
