from setuptools import setup

setup(
    name = "HEAVyBASE Hub",
    version = "5.8.4.0",
    author = "Luca Clivio",
    author_email = "luca.clivio@heavybase.org",
    description = ("Hybrid Online-Offline multiplatform P2P data entry engine "
                   "for electronic Case Report Forms and 'Omics' data sharing "
                   "based on a historiographical \"Push-based\" Peer-to-Peer DB"
                  ),
    license = "GPLv3",
    url = "https://github.com/lucaclivio/HEAVyBASE",
    py_modules=['HeavyBaseService', 'HeavyBaseService_updates'],
)

