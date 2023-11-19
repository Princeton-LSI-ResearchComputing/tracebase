from django.db import models


class Protocol(models.Model):
    ANIMAL_TREATMENT = "animal_treatment"
    CATEGORY_CHOICES = [
        (ANIMAL_TREATMENT, "Animal Treatment"),
    ]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="Unique name of the protocol.",
    )
    description = models.TextField(
        blank=True,
        null=True,
        help_text="Full text of the protocol's methods.",
    )
    category = models.CharField(
        max_length=256,
        choices=CATEGORY_CHOICES,
        help_text="Classification of the protocol, " "e.g. an animal treatment.",
    )

    @classmethod
    def retrieve_or_create_protocol(
        cls,
        protocol_input,
        category=None,
        provisional_description=None,
    ):
        """
        retrieve or create a protocol, based on input.
        protocol_input can either be a name or an integer (protocol_id)
        """

        if protocol_input is None:
            raise ValueError("protocol_input cannot be None.")

        created = False

        try:
            protocol = Protocol.objects.get(id=protocol_input)
        except ValueError:
            # protocol_input must not be an integer; try the name
            try:
                protocol, created = Protocol.objects.get_or_create(
                    name=protocol_input,
                    category=category,
                )
                if created:
                    # add the provisional description
                    if provisional_description is not None:
                        protocol.description = provisional_description
                        protocol.full_clean()
                        protocol.save()

            except Protocol.DoesNotExist as e:
                raise Protocol.DoesNotExist(
                    f"Protocol ID {protocol_input} does not exist."
                ) from e

        except Protocol.DoesNotExist as e:
            # protocol_input was an integer, but was not found
            print(f"Protocol ID {protocol_input} does not exist.")
            raise e
        return (protocol, created)

    class Meta:
        verbose_name = "protocol"
        verbose_name_plural = "protocols"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)
