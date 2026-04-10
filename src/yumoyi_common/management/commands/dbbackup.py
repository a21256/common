"""Django management command: python manage.py dbbackup"""

from django.core.management.base import BaseCommand, CommandError

from yumoyi_common.django_db_backup import (
    backup_current_database,
    cleanup_current_database_backups,
)


class Command(BaseCommand):
    help = "Backup the database using mysqldump"

    def add_arguments(self, parser):
        parser.add_argument(
            "--output-dir", required=True,
            help="Directory to store the backup file",
        )
        parser.add_argument(
            "--tables", nargs="*", default=None,
            help="Specific tables to backup (omit for full database)",
        )
        parser.add_argument(
            "--compress", action="store_true", default=False,
            help="Compress output with gzip (.sql.gz)",
        )
        parser.add_argument(
            "--database", default="default",
            help="Django database alias (default: 'default')",
        )
        parser.add_argument(
            "--cleanup", type=int, default=0, metavar="N",
            help="After backup, keep only the most recent N backups (0 = no cleanup)",
        )

    def handle(self, *args, **options):
        result = backup_current_database(
            output_dir=options["output_dir"],
            tables=options["tables"],
            compress=options["compress"],
            db_alias=options["database"],
        )

        if not result.success:
            raise CommandError(f"Backup failed: {result.error}")

        self.stdout.write(self.style.SUCCESS(
            f"Backup saved to {result.file_path} "
            f"({result.file_size} bytes, {result.duration:.1f}s)"
        ))

        if options["cleanup"] > 0:
            deleted = cleanup_current_database_backups(
                output_dir=options["output_dir"],
                db_alias=options["database"],
                keep=options["cleanup"],
            )
            if deleted:
                self.stdout.write(f"Cleaned up {deleted} old backup(s)")
