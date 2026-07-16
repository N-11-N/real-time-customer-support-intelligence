import great_expectations as gx
import great_expectations.expectations as gxe


def run_quality_gate(pdf):
    """Execute a real GX checkpoint on the Silver candidate batch."""
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("support_source")
    asset = source.add_dataframe_asset(name="support_events")
    batch = asset.add_batch_definition_whole_dataframe("whole_batch")
    suite = context.suites.add(gx.ExpectationSuite(name="support_quality_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="event_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="ticket_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="channel", value_set=["chat", "email", "web"]))
    suite.add_expectation(gxe.ExpectColumnValueLengthsToBeBetween(column="message", min_value=10, max_value=5000))
    definition = context.validation_definitions.add(gx.ValidationDefinition(
        name="support_validation", data=batch, suite=suite))
    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name="support_checkpoint", validation_definitions=[definition]))
    result = checkpoint.run(batch_parameters={"dataframe": pdf})
    return bool(result.success), result

