"""Django management command: python manage.py dbbackup"""

from django.core.management.base import BaseCommand, CommandError

from yumoyi_common.db_backup import DEFAULT_MYSQL, DEFAULT_MYSQLDUMP
from yumoyi_common.django_db_backup import (
    backup_current_database,
    cleanup_current_database_backups,
    list_current_database_tables,
)


class Command(BaseCommand):
    help = "Backup the database using mysqldump"

    def add_arguments(self, parser):
        parser.add_argument(
            "--output-dir", default=None,
            help="Directory to store the backup file (required unless --list-tables)",
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
        parser.add_argument(
            "--list-tables", action="store_true", default=False,
            help="List all tables in the database and exit (no backup)",
        )
        parser.add_argument(
            "--mysqldump-path", default=DEFAULT_MYSQLDUMP,
            help=f"Path to mysqldump binary (default: '{DEFAULT_MYSQLDUMP}' from PATH)",
        )
        parser.add_argument(
            "--mysql-path", default=DEFAULT_MYSQL,
            help=f"Path to mysql binary, used by --list-tables (default: '{DEFAULT_MYSQL}' from PATH)",
        )

    def handle(self, *args, **options):
        if options["list_tables"]:
            result = list_current_database_tables(
                db_alias=options["database"],
                mysql_path=options["mysql_path"],
            )
            if not result.success:
                raise CommandError(f"Failed to list tables: {result.error}")
            if not result.tables:
                self.stdout.write("No tables found.")
                return
            self.stdout.write(f"Tables in database ({len(result.tables)}):")
            for t in result.tables:
                self.stdout.write(f"  {t}")
            return

        if not options["output_dir"]:
            raise CommandError("--output-dir is required for backup")

        result = backup_current_database(
            output_dir=options["output_dir"],
            tables=options["tables"],
            compress=options["compress"],
            db_alias=options["database"],
            mysqldump_path=options["mysqldump_path"],
            mysql_path=options["mysql_path"],
        )

        if not result.success:
            raise CommandError(f"Backup failed: {result.error}")

        self.stdout.write(self.style.SUCCESS(
            f"Backup saved to {result.file_path} "
            f"({result.file_size} bytes, {result.duration:.1f}s)"
        ))

        if result.metadata:
            m = result.metadata
            self.stdout.write(f"  Tables: {m.table_count}")
            for ts in m.table_stats:
                self.stdout.write(
                    f"    {ts.name:<40s} ~{ts.estimated_row_count:>10,} rows"
                )

        if options["cleanup"] > 0:
            deleted = cleanup_current_database_backups(
                output_dir=options["output_dir"],
                db_alias=options["database"],
                keep=options["cleanup"],
            )
            if deleted:
                self.stdout.write(f"Cleaned up {deleted} old backup(s)")
