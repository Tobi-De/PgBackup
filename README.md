# pgbackup

[![pypi](https://img.shields.io/pypi/v/foxglove-web.svg)](https://pypi.python.org/pypi/foxglove-web)
[![versions](https://img.shields.io/pypi/pyversions/foxglove-web.svg)](https://github.com/samuelcolvin/foxglove)
[![license](https://img.shields.io/github/license/samuelcolvin/foxglove.svg)](https://github.com/samuelcolvin/foxglove/blob/master/LICENSE)

A cli program to automate your postgresql databases backups.

## Install

Install pgbackup with pip

```bash 
  pip install pgbackup
```
    
## Usage/Example

```shell
  pgbackup add-job
```

  
## Features

- schedule automatic database backups
- manage mutliple databases and servers at the same time
- restore a backup
- notify users on each backup and fails if it occurs

  
## Environment Variables
 
These are the following available environment variables.

| name                 | type          | default                    |
|----------------------|---------------|----------------------------|
| TIMEZONE             | str           | UTC                        |
| STORAGE_ENGINE       | str           | LOCAL                      |
| KEEP_MOST_RECENT     | int           | 5                          |
| LOCAL_BACKUP_FOLDER  | str           | f'{HOME}/pgbackup/backups' |
| AWS_BUCKET_NAME      | Optional[str] | None                       |
| AWS_BUCKET_PATH      | Optional[str] | None                       |
| PGB_GPG_RECIPIENT    | Optional[str] | None                       |
| PGB_GPG_ALWAYS_TRUST | bool          | False                      |

## Credits

- [django-dbbackup](https://github.com/django-dbbackup)
- [postgres_manager.py](https://gist.github.com/valferon/4d6ebfa8a7f3d4e84085183609d10f14)


## Release Notes

### Latest changes

### 0.1.0

- First release on pypi
