#!/usr/bin/env python

import os
import sys
from autoprocess import autoProcessTV, autoProcessMovie, autoProcessTVSR, sonarr, radarr, bonarr
from readSettings import ReadSettings
from mkvtomp4 import MkvtoMp4
from deluge_client import DelugeRPCClient
import logging
import time
from logging.config import fileConfig
from unrar_helper import ProcessFile as PF

logpath = './logs/deluge_convert'
if os.name == 'nt':
    logpath = os.path.dirname(sys.argv[0])
elif not os.path.isdir(logpath):
    try:
        os.mkdir(logpath)
    except:
        logpath = os.path.dirname(sys.argv[0])

#settings = ReadSettings(os.path.dirname(sys.argv[0]), "autoProcess.ini")
#categories = [settings.deluge['sb'], settings.deluge['cp'], settings.deluge['sonarr'], settings.deluge['radarr'], settings.deluge['bonarr'], settings.deluge['sr'], settings.deluge['bypass']]
#remove = settings.deluge['remove']

def main(argv):
    logpath = './logs/deluge_convert'
    if os.name == 'nt':
        logpath = os.path.dirname(sys.argv[0])
    elif not os.path.isdir(logpath):
        try:
            os.mkdir(logpath)
        except:
            logpath = os.path.dirname(sys.argv[0])

    configPath = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), 'logging.ini')).replace("\\", "\\\\")
    logPath = os.path.abspath(os.path.join(logpath, 'index.log')).replace("\\", "\\\\")
    fileConfig(configPath, defaults={'logfilename': logPath})
    log = logging.getLogger("delugePostProcess")

    log.info("Deluge post processing started.")

    settings = ReadSettings(os.path.dirname(sys.argv[0]), "autoProcess.ini")
    log.info('Settings: %s', [settings.deluge['sb'], settings.deluge['cp'], settings.deluge['sonarr'], settings.deluge['radarr'], settings.deluge['sr'], settings.deluge['bypass'], settings.deluge['bonarr']])
    categories = [settings.deluge['sb'], settings.deluge['cp'], settings.deluge['sonarr'], settings.deluge['radarr'], settings.deluge['sr'], settings.deluge['bypass'], settings.deluge['bonarr']]
    remove = settings.deluge['remove']

    if len(argv) < 4:
        log.error("Not enough command line parameters present, are you launching this from deluge?")
        return
    else:
        path = str(argv[3])
        torrent_name = str(argv[2])
        torrent_id = str(argv[1])

        log.info("Path: %s." % path)
        log.info("Torrent: %s." % torrent_name)
        log.info("Hash: %s." % torrent_id)

    client = DelugeRPCClient(host=settings.deluge['host'], port=int(settings.deluge['port']), username=settings.deluge['user'], password=settings.deluge['pass'])
    client.connect()

    if client.connected:
      log.info("Successfully connected to Deluge")
    else:
      log.error("Failed to connect to Deluge")
      sys.exit()

    torrent_data = client.call('core.get_torrent_status', torrent_id, ['files', 'label'])
    torrent_files = torrent_data[b'files']
    category = torrent_data[b'label'].lower().decode()

    files = []
    # log.debug("List of files in torrent:")
    for contents in torrent_files:
        files.append(contents[b'path'].decode())
        # log.debug(contents[b'path'].decode())

    if category.lower() not in categories:
        log.error("No valid category detected.")
        sys.exit()

    if len(categories) != len(set(categories)):
        log.error("Duplicate category detected. Category names must be unique.")
        sys.exit()

    if settings.deluge['convert']:
    # Check for custom Deluge output_dir
        if settings.deluge['output_dir']:
            settings.output_dir = os.path.join(settings.deluge['output_dir'], "%s" % torrent_name)
            log.debug("Overriding output_dir to %s.", settings.deluge['output_dir'])

    # Perform conversion.

        settings.delete = False
        if not settings.output_dir:
            suffix = "convert"
            settings.output_dir = os.path.join(path, ("%s-%s" % (torrent_name, suffix)))
            if not os.path.exists(settings.output_dir):
                os.mkdir(settings.output_dir)
            delete_dir = settings.output_dir

        converter = MkvtoMp4(settings)

        filelist = PF(files)

#        log.info('File List, %s', filelist.filelist)
        filelist = filelist.find_files(path)
        movies = filelist['movies']
        archive = filelist['archive']

#        log.info('movies %s', movies)
        log.info('archive %s', archive)

        while len(movies) == 0:
            if len(archive) == 1:
                log.info('We have 1 file in our archive list, extracting it')
                pfiles = PF(archive[0])
                if pfiles.extract_archive(archive[0],settings.output_dir) == True:
                    log.info('Our archive was successful!')
                    filelist = PF(settings.output_dir)
                    log.info('our filelist.files %s', filelist.files)
                    filelist = filelist.scan_dir(path)
                    movies = filelist['movies']
                    archive = filelist['archive']
                    converter.delete = True
                    log.info('Our movies list is %s', movies)
                    log.info('Our Archive list is %s', archive)
                else:
                    log.error('Our extraction failed')
                    break

            elif len(archive) >= 1:
                log.info('We have lots of files in our archive list')
                log.info('Choosing the first file to avoid decompressing same file multiple times.')
                if pfiles.extract_archive(archive[0],settings.output_dir) == True:
                    log.info('Our archive was successful!')
                    filelist = PF(settings.output_dir)
                    log.info('our filelist.files %s', filelist.files)
                    filelist = filelist.scan_dir(path)
                    movies = filelist['movies']
                    archive = filelist['archive']
                    converter.delete = True
                    log.info('Our movies list is %s', movies)
                    log.info('Our Archive list is %s', archive)
                else:
                    log.error('Our extraction failed')
                    break
            else:
                log.error('We have no files to process')
                break

        log.info('our movie length is %s', len(movies))
        if len(movies) >= 1:
            log.info('We have %s files in our movie list', len(movies))
            if not os.path.exists(settings.output_dir):
                os.mkdir(settings.output_dir)

            for f in movies:
                try:
                    log.info("Converting file %s at location %s.", f, settings.output_dir)
                    output = converter.process(f)
                except:
                    log.exception("Error converting file %s.", f)
            delete_dir = settings.output_dir
        path = converter.output_dir
    else:
        suffix = "copy"
        newpath = os.path.join(path, ("%s-%s" % (torrent_name, suffix)))
        if not os.path.exists(newpath):
            os.mkdir(newpath)
        for filename in files:
            inputfile = os.path.join(path, filename)
            log.info("Copying file %s to %s." % (inputfile, newpath))
            shutil.copy(inputfile, newpath)
        path = newpath
        delete_dir = newpath

# Send to Sickbeard
    if (category == categories[0]):
        log.info("Passing %s directory to Sickbeard." % path)
        autoProcessTV.processEpisode(path, settings)
# Send to CouchPotato
    elif (category == categories[1]):
        log.info("Passing %s directory to Couch Potato." % path)
        autoProcessMovie.process(path, settings, torrent_name)
# Send to Sonarr
    elif (category == categories[2]):
        log.info("Passing %s directory to Sonarr." % path)
        sonarr.processEpisode(path, settings)
        log.info('Waiting %s seconds to let Sonarr finish importing', len(movies)*10 )
        time.sleep(len(movies)*10)
    elif (category == categories[3]):
        log.info("Passing %s directory to Radarr." % path)
        radarr.processMovie(path, settings)
        log.info('Waiting 10 seconds to let Radarr finish importing')
        time.sleep(10)
    elif (category == categories[6]):
        log.info("Passing %s directory to Bonarr." % path)
        bonarr.processMovie(path, settings)
        log.info('Waiting 10 seconds to let Bonarr finish importing')
        time.sleep(10)
    elif (category == categories[4]):
        log.info("Passing %s directory to Sickrage." % path)
        autoProcessTVSR.processEpisode(path, settings)
    elif (category == categories[5]):
        log.info("Bypassing any further processing as per category.")

    if delete_dir:
        if os.path.exists(delete_dir):
            try:
                os.rmdir(delete_dir)
                log.debug("Successfully removed tempoary directory %s." % delete_dir)
                return
            except:
                log.exception("Unable to delete temporary directory.")
                pass

    if remove:
        try:
            client.call('core.remove_torrent', torrent_id, True)
            return
        except:
            log.exception("Unable to remove torrent from deluge.")
            pass

if __name__ == "__main__":
    main(sys.argv)
