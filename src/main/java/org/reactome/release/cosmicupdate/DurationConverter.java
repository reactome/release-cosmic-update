package org.reactome.release.cosmicupdate;

import java.time.Duration;

import com.beust.jcommander.IStringConverter;

/**
 * Custom converter class for JCommander.
 * It will convert a String from the command line to a Duration.
 * @author sshorser
 *
 */
class DurationConverter implements IStringConverter<Duration>
{
	@Override
	public Duration convert(String value)
	{
		return Duration.parse(value);
	}
}