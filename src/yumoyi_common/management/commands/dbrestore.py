"""Django management command: python manage.py dbrestore"""

from django.core.management.base import BaseCommand, CommandError

from yumoyi_common.db_backup import DEFAULT_MYSQL, DEFAULT_TIMEOUT_SECONDS
from yumoyi_common.django_db_backup import restore_to_current_database

_DEFAULT_DB_ALIAS = "default"


class Command(BaseCommand):
    help = "Restore the database from a mysqldump backup file"

    def add_arguments(self, parser):
        parser.add_argument(
            "backup_file",
            help="Path to the .sql or .sql.gz backup file",
        )
        parser.add_argument(
            "--database", default=_DEFAULT_DB_ALIAS,
            help="Django database alias (default: 'default')",
        )
        parser.add_argument(
            "--mysql-path", default=DEFAULT_MYSQL,
            help=f"Path to mysql binary (default: '{DEFAULT_MYSQL}' from PATH)",
        )
        parser.add_argument(
            "--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS,
            help=f"Timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS})",
        )
        parser.add_argument(
            "--extra-args", nargs="*", default=[],
            help="Extra arguments to pass to mysql client",
        )

    def handle(self, *args, **options):
        result = restore_to_current_database(
            backup_file=options["backup_file"],
            db_alias=options["database"],
            mysql_path=options["mysql_path"],
            timeout=options["timeout"],
            extra_args=options["extra_args"],
        )

        if not result.success:
            raise CommandError(f"Restore failed: {result.error}")

        self.stdout.write(self.style.SUCCESS(
            f"Restore completed in {result.duration:.1f}s"
        ))
