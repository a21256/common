"""Django management command: python manage.py dbrestore"""

from django.core.management.base import BaseCommand, CommandError

from yumoyi_common.django_db_backup import restore_to_current_database


class Command(BaseCommand):
    help = "Restore the database from a mysqldump backup file"

    def add_arguments(self, parser):
        parser.add_argument(
            "backup_file",
            help="Path to the .sql or .sql.gz backup file",
        )
        parser.add_argument(
            "--database", default="default",
            help="Django database alias (default: 'default')",
        )
        parser.add_argument(
            "--mysql-path", default="mysql",
            help="Path to mysql binary (default: 'mysql' from PATH)",
        )

    def handle(self, *args, **options):
        result = restore_to_current_database(
            backup_file=options["backup_file"],
            db_alias=options["database"],
            mysql_path=options["mysql_path"],
        )

        if not result.success:
            raise CommandError(f"Restore failed: {result.error}")

        self.stdout.write(self.style.SUCCESS(
            f"Restore completed in {result.duration:.1f}s"
        ))
